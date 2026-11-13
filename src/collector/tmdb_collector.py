#!/usr/bin/env python3

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
    from dotenv import load_dotenv
except ImportError:
    sys.exit("Установите зависимости: pip install -r requirements.txt")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_json(path: Path, data):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def append_jsonl(path: Path, obj):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")


def iter_jsonl(path: Path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


# ============================================================
# HTTP-клиент
# ============================================================

class TMDBClient:
    BASE = "https://api.themoviedb.org/3"

    def __init__(self, token: str, base_delay: float = 0.05, max_delay: float = 5.0):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "accept": "application/json",
        })
        self.base_delay = base_delay
        self.current_delay = base_delay
        self.max_delay = max_delay
        self.requests = 0
        self.retries = 0
        self.rate_limited = 0

    def _sleep(self):
        time.sleep(self.current_delay + random.uniform(0, self.current_delay * 0.3))

    def get(self, path: str, params: dict = None, max_retries: int = 6):
        url = f"{self.BASE}{path}"
        for attempt in range(1, max_retries + 1):
            self._sleep()
            try:
                r = self.session.get(url, params=params, timeout=20)
            except requests.RequestException as e:
                self.retries += 1
                wait = min(self.max_delay, 0.5 * (2 ** attempt))
                print(f"  ! сеть: {e}. ждём {wait:.1f}с", flush=True)
                time.sleep(wait)
                continue

            self.requests += 1

            if r.status_code == 200:
                self.current_delay = max(self.base_delay, self.current_delay * 0.9)
                return r.json()

            if r.status_code == 429:
                self.rate_limited += 1
                self.retries += 1
                self.current_delay = min(self.max_delay, self.current_delay * 2)
                retry_after = float(r.headers.get("Retry-After", 0) or 0)
                wait = max(retry_after, self.current_delay)
                print(f"  ! 429. delay={self.current_delay:.2f}s, ждём {wait:.1f}с", flush=True)
                time.sleep(wait)
                continue

            if r.status_code == 404:
                return None

            if 500 <= r.status_code < 600:
                self.retries += 1
                time.sleep(min(self.max_delay, 0.5 * (2 ** attempt)))
                continue

            print(f"  ! HTTP {r.status_code} на {url}", flush=True)
            return None

        print(f"  ! не удалось после {max_retries} попыток: {url}", flush=True)
        return None


# ============================================================
# Валидация и метрики
# ============================================================

def validate_movie(movie: dict, expected_id: int):
    if not isinstance(movie, dict):
        return False, "not a dict"
    if movie.get("id") != expected_id:
        return False, "id mismatch"
    credits = movie.get("credits")
    if not isinstance(credits, dict):
        return False, "no credits"
    cast, crew = credits.get("cast"), credits.get("crew")
    if not isinstance(cast, list) or not isinstance(crew, list):
        return False, "no cast/crew arrays"
    for group in (cast, crew):
        for p in group:
            if not isinstance(p, dict) or not isinstance(p.get("id"), int):
                return False, "invalid participant id"
    return True, None


def has_financials(movie: dict) -> bool:
    b = movie.get("budget")
    r = movie.get("revenue")
    return (
        isinstance(b, (int, float)) and b > 0
        and isinstance(r, (int, float)) and r > 0
    )


# ============================================================
# Сбор ID — с поддержкой per_year
# ============================================================

def discover_movie_ids(client, start_year, end_year, min_votes, language,
                       per_year=None):
    """
    Генератор ID фильмов.
    Если per_year=None — собирает все ID за каждый год (full).
    Если per_year=N — берёт первые N ID на каждый год (pilot).
    """
    total = 0
    for year in range(start_year, end_year + 1):
        year_ids = []
        page = 1
        total_pages = 1
        while page <= total_pages:
            data = client.get("/discover/movie", {
                "language": language,
                "sort_by": "popularity.desc",
                "primary_release_year": year,
                "vote_count.gte": min_votes,
                "page": page,
                "include_adult": "false",
            })
            if not data:
                break
            total_pages = min(int(data.get("total_pages", 1)), 500)
            results = data.get("results", [])
            for m in results:
                mid = m.get("id")
                if isinstance(mid, int):
                    year_ids.append(mid)
                    if per_year and len(year_ids) >= per_year:
                        break
            if per_year and len(year_ids) >= per_year:
                break
            page += 1

        total += len(year_ids)
        print(f"  год {year}: {len(year_ids)} ID (всего {total})", flush=True)

        for mid in year_ids:
            yield mid


# ============================================================
# collect
# ============================================================

def collect(args):
    load_dotenv(Path(__file__).resolve().parents[2] / ".env", encoding="utf-8-sig")
    token = os.getenv("TMDB_TOKEN", "").strip()
    if not token or token.startswith(("YOUR_", "PASTE_")):
        sys.exit("Задайте TMDB_TOKEN в .env в корне проекта или в переменных окружения.")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    movies_path = out / "movies.jsonl"
    errors_path = out / "errors.jsonl"
    runs_path = out / "runs.jsonl"
    summary_path = out / "summary.json"

    received = set()
    if movies_path.exists():
        for m in iter_jsonl(movies_path):
            if isinstance(m.get("id"), int):
                received.add(m["id"])
        print(f"Уже получено ранее: {len(received)} фильмов", flush=True)

    known_errors = set()
    if errors_path.exists():
        for e in iter_jsonl(errors_path):
            if isinstance(e.get("movie_id"), int):
                known_errors.add(e["movie_id"])

    client = TMDBClient(token, base_delay=args.delay)
    started = time.perf_counter()

    per_year = args.pilot_per_year if args.mode == "pilot" else None
    print(f"Собираем список ID (mode={args.mode}, "
          f"{args.start_year}-{args.end_year}, "
          f"min_votes={args.min_votes}"
          + (f", per_year={per_year})" if per_year else ")"), flush=True)

    all_ids = list(discover_movie_ids(
        client, args.start_year, args.end_year, args.min_votes,
        args.language, per_year=per_year
    ))
    print(f"Итого найдено ID: {len(all_ids)}", flush=True)

    run = {
        "started_at": utc_now(),
        "mode": args.mode,
        "start_year": args.start_year,
        "end_year": args.end_year,
        "min_votes": args.min_votes,
        "require_financials": args.require_financials,
    }
    stats = {
        "movies_received": len(received),
        "skipped_no_financials": 0,
        "skipped_already_received": 0,
        "skipped_known_errors": 0,
        "errors": 0,
    }

    status = "finished"
    try:
        processed = 0
        with movies_path.open("a", encoding="utf-8") as out_stream:
            for idx, movie_id in enumerate(all_ids):
                if movie_id in received:
                    stats["skipped_already_received"] += 1
                    continue
                if movie_id in known_errors:
                    stats["skipped_known_errors"] += 1
                    continue

                movie = client.get(
                    f"/movie/{movie_id}",
                    {"language": args.language, "append_to_response": "credits"},
                )
                if movie is None:
                    append_jsonl(errors_path, {
                        "stage": "http", "movie_id": movie_id,
                        "error": "no response", "at": utc_now(),
                    })
                    stats["errors"] += 1
                    continue

                ok, reason = validate_movie(movie, movie_id)
                if not ok:
                    append_jsonl(errors_path, {
                        "stage": "validate", "movie_id": movie_id,
                        "error": reason, "at": utc_now(),
                    })
                    stats["errors"] += 1
                    continue

                if args.require_financials and not has_financials(movie):
                    stats["skipped_no_financials"] += 1
                    continue

                movie["_fetched_at"] = utc_now()
                movie["_language"] = args.language
                movie["_source"] = "TMDB API v3"

                out_stream.write(
                    json.dumps(movie, ensure_ascii=False, separators=(",", ":")) + "\n"
                )
                out_stream.flush()
                received.add(movie_id)
                stats["movies_received"] = len(received)

                if not (out / "example_movie.json").exists():
                    write_json(out / "example_movie.json", movie)

                processed += 1
                if processed % 50 == 0:
                    pct = 100 * idx / len(all_ids) if all_ids else 0
                    print(
                        f"  [{pct:5.1f}%] +{processed} | всего {len(received)} | "
                        f"нет финансов {stats['skipped_no_financials']} | "
                        f"ошибок {stats['errors']} | "
                        f"delay {client.current_delay:.2f}s | 429: {client.rate_limited}",
                        flush=True,
                    )

    except KeyboardInterrupt:
        status = "interrupted"
        print("\n[Ctrl+C] Остановлено. Запустите ту же команду снова — продолжит.",
              flush=True)
    except BaseException:
        status = "aborted"
        raise
    finally:
        elapsed = round(time.perf_counter() - started, 2)
        run.update({
            "ended_at": utc_now(),
            "status": status,
            "elapsed_seconds": elapsed,
            "http_requests": client.requests,
            "retries": client.retries,
            "rate_limited_429": client.rate_limited,
            "final_delay": round(client.current_delay, 3),
        })
        append_jsonl(runs_path, run)

        summary = build_summary(out, stats, elapsed)
        write_json(summary_path, summary)

        print("\n" + "=" * 60)
        print(f"ГОТОВО: {summary['movies_received']} фильмов")
        print(f"  Размер movies.jsonl: {summary['movies_jsonl_size_MB']} МБ")
        print(f"  Пропущено (нет budget/revenue): {summary['skipped_no_financials']}")
        print(f"  Ошибок: {summary['errors']}")
        print(f"  Время: {summary['elapsed_seconds']} сек")
        print(f"  Summary: {summary_path}")
        print("=" * 60)


def build_summary(out: Path, stats: dict, elapsed: float) -> dict:
    movies_path = out / "movies.jsonl"
    size_bytes = movies_path.stat().st_size if movies_path.exists() else 0
    return {
        "generated_at": utc_now(),
        "movies_received": stats["movies_received"],
        "skipped_no_financials": stats["skipped_no_financials"],
        "skipped_already_received": stats["skipped_already_received"],
        "skipped_known_errors": stats["skipped_known_errors"],
        "errors": stats["errors"],
        "elapsed_seconds": elapsed,
        "movies_jsonl_size_MB": round(size_bytes / (1024 * 1024), 2),
    }


# ============================================================
# stats
# ============================================================

def stats_cmd(args):
    out = Path(args.out)
    summary_path = out / "summary.json"
    if not summary_path.exists():
        sys.exit(f"Нет {summary_path}. Сначала запустите collect.")
    data = json.loads(summary_path.read_text(encoding="utf-8"))
    print(json.dumps(data, ensure_ascii=False, indent=2))


# ============================================================
# main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Сборщик данных TMDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("collect", help="Скачать фильмы и credits из TMDB")
    p.add_argument("--mode", choices=["pilot", "full"], default="full")
    p.add_argument("--start-year", type=int, default=1900,
                   help="с какого года (по умолчанию 1900)")
    p.add_argument("--end-year", type=int, default=2026)
    p.add_argument("--min-votes", type=int, default=50,
                   help="минимум голосов TMDB (по умолчанию 50)")
    p.add_argument("--language", default="en-US",
                   help="ru-RU или en-US")
    p.add_argument("--delay", type=float, default=0.01,
                   help="базовая пауза между запросами, сек")
    p.add_argument("--pilot-per-year", type=int, default=2,
                   help="сколько ID на год в pilot-режиме (по умолчанию 2)")
    p.add_argument("--out", type=Path, default=Path("data"))
    p.add_argument("--no-require-financials", action="store_true",
                   help="НЕ пропускать фильмы без budget/revenue")

    p = sub.add_parser("stats", help="Показать summary.json")
    p.add_argument("--out", type=Path, default=Path("data"))

    args = parser.parse_args()

    if args.command == "collect":
        args.require_financials = not args.no_require_financials
        collect(args)
    elif args.command == "stats":
        stats_cmd(args)


if __name__ == "__main__":
    main()
