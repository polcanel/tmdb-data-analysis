#!/usr/bin/env python3
"""Очистка TMDB movies.jsonl через PySpark и загрузка в MongoDB.

Python 3.10–3.12, Java 17, PySpark 3.5.6.
Первый запуск: python tmdb_clean_load.py --input data/movies.jsonl --dry-run
Загрузка:     python tmdb_clean_load.py --input data/movies.jsonl
"""
import argparse
import hashlib
import json
import math
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path

CONNECTOR = "org.mongodb.spark:mongo-spark-connector_2.12:10.7.0"


def text(value):
    return value.strip() or None if isinstance(value, str) else None


def integer(value, minimum=0):
    """Не превращаем bool, дробные числа и повреждённые значения в ID."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
        if not math.isfinite(number) or not number.is_integer():
            return None
        result = int(value)
        return result if minimum <= result <= 2**63 - 1 else None
    except (ValueError, TypeError, OverflowError):
        return None


def real(value, minimum=0, maximum=None):
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = float(value)
        if not math.isfinite(result) or result < minimum:
            return None
        return result if maximum is None or result <= maximum else None
    except (ValueError, TypeError, OverflowError):
        return None


def clean_line(line):
    """Выполняется на Spark workers. Возвращает (статус, JSON).

    error сохраняет исходную строку; пустые строки учитываются отдельно.
    Исходный файл не изменяется. Нужные для анализа поля перечислены явно.
    """
    if not line.strip():
        return "blank", ""
    try:
        source = json.loads(line, parse_constant=lambda v: (_ for _ in ()).throw(ValueError(v)))
        if not isinstance(source, dict):
            raise ValueError("record_is_not_object")
        movie_id = integer(source.get("id"), 1)
        if movie_id is None:
            raise ValueError("invalid_movie_id")
    except (ValueError, TypeError, RecursionError) as exc:
        return "error", json.dumps({"reason": str(exc), "raw": line}, ensure_ascii=False)

    issues = []
    movie = {"_id": movie_id, "id": movie_id}
    for field in ("title", "original_title", "original_language", "overview", "imdb_id", "status"):
        movie[field] = text(source.get(field))
    if not movie["title"]:
        issues.append("missing_title")

    release = text(source.get("release_date"))
    try:
        if not release or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", release):
            raise ValueError
        date.fromisoformat(release)
        movie["release_date"] = release
        movie["release_year"] = int(release[:4])
    except ValueError:
        movie["release_date"] = movie["release_year"] = None
        issues.append("missing_or_invalid_release_date")

    for field in ("budget", "revenue", "runtime"):
        movie[field] = integer(source.get(field), 1)
        if movie[field] is None:
            issues.append("missing_or_invalid_" + field)
    movie["vote_count"] = integer(source.get("vote_count"))
    movie["vote_average"] = real(source.get("vote_average"), maximum=10)
    movie["popularity"] = real(source.get("popularity"))
    for field in ("adult", "video"):
        movie[field] = source.get(field) if isinstance(source.get(field), bool) else None

    def objects(field, key, country=False):
        items = source.get(field)
        if not isinstance(items, list):
            issues.append("missing_or_invalid_" + field)
            return []
        result, seen = [], set()
        for item in items:
            value = text(item.get(key)) if country and isinstance(item, dict) else (
                integer(item.get(key), 1) if isinstance(item, dict) else None)
            if country and value:
                value = value.upper()
                if not re.fullmatch(r"[A-Z]{2}", value):
                    value = None
            if value is None:
                issues.append("invalid_item_" + field)
                continue
            if value not in seen:
                seen.add(value)
                result.append({key: value, "name": text(item.get("name"))})
        return result

    movie["genres"] = objects("genres", "id")
    movie["production_countries"] = objects("production_countries", "iso_3166_1", country=True)
    movie["production_companies"] = objects("production_companies", "id")
    movie["has_production_countries"] = bool(movie["production_countries"])
    movie["is_coproduction"] = (len(movie["production_countries"]) > 1
                                if movie["has_production_countries"] else None)

    credits = source.get("credits")
    credits = credits if isinstance(credits, dict) else {}
    movie["has_credits"] = all(isinstance(credits.get(k), list) for k in ("cast", "crew"))
    if not movie["has_credits"]:
        issues.append("missing_or_invalid_credits")
    cleaned_credits = {}
    for group in ("cast", "crew"):
        rows = credits.get(group)
        rows = rows if isinstance(rows, list) else []
        result = []
        for person in rows:
            pid = integer(person.get("id"), 1) if isinstance(person, dict) else None
            if pid is None:
                issues.append("invalid_participant_id_" + group)
                continue
            gender = integer(person.get("gender"))
            if gender not in (0, 1, 2, 3):
                gender = 0
                issues.append("missing_or_invalid_gender_" + group)
            p = {"id": pid, "name": text(person.get("name")), "gender": gender,
                 "credit_id": text(person.get("credit_id")),
                 "known_for_department": text(person.get("known_for_department"))}
            if group == "cast":
                p.update(character=text(person.get("character")), order=integer(person.get("order")))
            else:
                p.update(department=text(person.get("department")), job=text(person.get("job")))
            result.append(p)
        cleaned_credits[group] = result
    movie["credits"] = cleaned_credits
    movie["has_financials"] = movie["budget"] is not None and movie["revenue"] is not None
    movie["quality_issues"] = sorted(set(issues))
    movie["source"] = text(source.get("_source")) or "TMDB API v3"
    movie["language"] = text(source.get("_language"))
    fetched = text(source.get("_fetched_at"))
    try:
        timestamp = datetime.fromisoformat(fetched.replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            raise ValueError
        fetched = timestamp.astimezone(timezone.utc).isoformat(timespec="microseconds")
    except (ValueError, AttributeError):
        fetched = None
    movie["fetched_at"] = fetched
    payload = json.dumps(movie, ensure_ascii=False, sort_keys=True, allow_nan=False)
    movie["record_hash"] = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return "ok", json.dumps(movie, ensure_ascii=False, allow_nan=False)


def schema():
    from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                                  DoubleType, BooleanType, ArrayType)
    def struct(fields):
        return StructType([StructField(name, kind, True) for name, kind in fields])
    S, L, D, B = StringType(), LongType(), DoubleType(), BooleanType()
    named_id = struct([("id", L), ("name", S)])
    country = struct([("iso_3166_1", S), ("name", S)])
    common = [("id", L), ("name", S), ("gender", L), ("credit_id", S), ("known_for_department", S)]
    cast = struct(common + [("character", S), ("order", L)])
    crew = struct(common + [("department", S), ("job", S)])
    return struct(
        [("_id", L), ("id", L)] +
        [(x, S) for x in ("title", "original_title", "original_language", "overview", "imdb_id",
                         "status", "release_date", "source", "language", "fetched_at", "record_hash")] +
        [(x, L) for x in ("release_year", "budget", "revenue", "runtime", "vote_count")] +
        [(x, D) for x in ("vote_average", "popularity")] +
        [(x, B) for x in ("adult", "video", "has_credits", "has_financials",
                         "has_production_countries", "is_coproduction")] +
        [("genres", ArrayType(named_id)), ("production_countries", ArrayType(country)),
         ("production_companies", ArrayType(named_id)),
         ("credits", struct([("cast", ArrayType(cast)), ("crew", ArrayType(crew))])),
         ("quality_issues", ArrayType(S))])


def main():
    from pyspark import StorageLevel
    from pyspark.sql import SparkSession, Window, functions as F
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Путь к movies.jsonl")
    parser.add_argument("--database", default="tmdb_coursework")
    parser.add_argument("--collection", default="movies_clean")
    parser.add_argument("--reports", default="cleaning_reports")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--dry-run", action="store_true", help="Проверка без записи в MongoDB")
    args = parser.parse_args()
    if args.partitions < 1:
        parser.error("--partitions должен быть положительным")
        
    uri = os.environ.get("MONGODB_URI", "mongodb://mongo:27017")
    
    builder = (
        SparkSession.builder.appName("TMDB cleaning and loading")
        .master(args.master)
        .config("spark.sql.shuffle.partitions", str(args.partitions))
        .config("spark.driver.memory", "3g")
        .config("spark.executor.memory", "3g")
    )
    
    if not args.dry_run:
        builder = (
            builder.config("spark.jars.packages", CONNECTOR)
            .config("spark.mongodb.write.connection.uri", uri)
        )
        

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    run_dir = Path(args.reports) / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    run_dir.mkdir(parents=True, exist_ok=False)
    parsed = clean = None
    try:
        parsed = (spark.read.text(args.input).rdd.map(lambda r: clean_line(r.value))
                  .persist(StorageLevel.DISK_ONLY))
        counts = dict(parsed.map(lambda r: (r[0], 1)).reduceByKey(lambda a, b: a + b).collect())
        if counts.get("error", 0):
            parsed.filter(lambda r: r[0] == "error").map(lambda r: r[1]).saveAsTextFile(
                str((run_dir / "rejected_jsonl").resolve()))
        if not counts.get("ok", 0):
            (run_dir / "summary.json").write_text(json.dumps(counts, indent=2), encoding="utf-8")
            raise RuntimeError("Нет допустимых записей. Проверьте summary.json и rejected_jsonl.")
        df = spark.read.schema(schema()).json(parsed.filter(lambda r: r[0] == "ok").map(lambda r: r[1]))
        window = Window.partitionBy("id").orderBy(F.col("fetched_at").desc_nulls_last(),
                                                  F.col("record_hash").desc())
        clean = (df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1")
                 .drop("_rn", "record_hash").persist(StorageLevel.DISK_ONLY))
        metrics = clean.agg(
            F.count("*").alias("movies"),
            F.sum(F.col("has_financials").cast("long")).alias("movies_with_financials"),
            F.sum(F.col("has_credits").cast("long")).alias("movies_with_credits"),
            F.sum(F.col("release_year").isNull().cast("long")).alias("movies_without_year"),
            F.sum((~F.col("has_production_countries")).cast("long")).alias("movies_without_countries")
        ).first().asDict()
        issue_counts = {r["issue"]: r["count"] for r in clean.select(
            F.explode("quality_issues").alias("issue")).groupBy("issue").count().collect()}
        summary = {"input": args.input, "dry_run": args.dry_run, "line_counts": counts,
                   "duplicates_removed": counts["ok"] - metrics["movies"],
                   **metrics, "movies_by_quality_issue": issue_counts, "mongo_write_completed": False}
        summary_path = run_dir / "summary.json"
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        if not args.dry_run:
            (clean.repartition(args.partitions).write.format("mongodb").mode("append")
             .option("database", args.database).option("collection", args.collection)
             .option("operationType", "replace").option("idFieldList", "_id")
             .option("upsertDocument", "true").option("maxBatchSize", "500").save())
            summary["mongo_write_completed"] = True
            summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"Загружено {metrics['movies']} фильмов в {args.database}.{args.collection}")
        print(f"Отчёт: {summary_path}")
    finally:
        if clean is not None:
            clean.unpersist()
        if parsed is not None:
            parsed.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
