"""Prépare le dataset journalier supervisé à partir de Gold."""

import argparse
import re
import sys
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from analytics.ml.features import (
    DEFAULT_AVAILABILITY_DAYS,
    attach_labels,
    calendar_references,
    canonical_observations,
    key_statistics,
    select_panel,
    temporal_features,
)
from analytics.ml.runtime import (
    DATASET_ROOT,
    GOLD_ROOT,
    TARGET,
    ensure_new_hdfs_path,
    print_manifest,
    start_spark,
    write_manifest,
)


def build_dataset(spark, args, output):
    source = spark.read.parquet(args.input)
    panel = select_panel(source, args.project, args.pages).where(
        F.col("event_date").between(args.start, args.end)
    )
    keys = key_statistics(panel).cache()
    normalized = canonical_observations(keys)
    references = calendar_references(
        spark, args.project, args.pages, args.start, args.end - timedelta(days=1)
    )
    labelled = attach_labels(
        temporal_features(references, normalized, args.availability_days), normalized
    ).cache()

    try:
        quality = (
            keys.agg(
                F.coalesce(
                    F.sum(
                        F.when(
                            F.col("min_views") == F.col("max_views"),
                            F.col("observations") - 1,
                        )
                    ),
                    F.lit(0),
                ).alias("identical_duplicates_removed"),
                F.coalesce(
                    F.sum(
                        F.when(
                            F.col("min_views") != F.col("max_views"),
                            F.col("observations"),
                        )
                    ),
                    F.lit(0),
                ).alias("conflicting_rows_removed"),
            )
            .first()
            .asDict()
        )
        summary = (
            labelled.agg(
                F.count("*").alias("calendar_references"),
                F.count(F.when(F.col(TARGET).isNull(), 1)).alias("no_label"),
                F.count(
                    F.when(
                        F.col(TARGET).isNotNull() & F.col("latest_views").isNull(), 1
                    )
                ).alias("labelled_without_history"),
            )
            .first()
            .asDict()
        )
        result = labelled.where(
            F.col(TARGET).isNotNull() & F.col("latest_views").isNotNull()
        )
        metadata = {
            "version": args.version,
            "gold": args.input,
            "gold_export_sha256": args.gold_export_sha256,
            "gold_rows": source.count(),
            "project": args.project,
            "pages": args.pages,
            "first_input_day": args.start,
            "last_input_day": args.end,
            "availability_days": args.availability_days,
            "scoring_deadline": "prediction_date 00:00 UTC exclusive",
            "raw_panel_rows": panel.count(),
            "valid_panel_rows_before_dedup": keys.agg(
                F.coalesce(F.sum("observations"), F.lit(0)).alias("rows")
            )
            .first()
            .rows,
            "canonical_panel_rows": normalized.count(),
            **quality,
            **summary,
            "supervised_rows": result.count(),
            "spark_version": spark.version,
        }
        if not metadata["supervised_rows"]:
            raise ValueError("Aucune observation supervisée utilisable")
        result.write.parquet(f"{output}/data")
        write_manifest(spark, f"{output}/manifest", metadata)
        print_manifest("WIKIMEDIA_SUPERVISED", metadata)
    finally:
        labelled.unpersist()
        keys.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=f"{GOLD_ROOT}/ml_dataset")
    parser.add_argument("--output-root", default=DATASET_ROOT)
    parser.add_argument("--version", required=True)
    parser.add_argument("--project", default="en.wikipedia")
    parser.add_argument("--pages", required=True)
    parser.add_argument("--start", required=True, type=date.fromisoformat)
    parser.add_argument("--end", required=True, type=date.fromisoformat)
    parser.add_argument(
        "--availability-days", type=int, default=DEFAULT_AVAILABILITY_DAYS
    )
    parser.add_argument("--gold-export-sha256", required=True)
    args = parser.parse_args()
    args.pages = sorted({page.strip() for page in args.pages.split(",")})
    if args.start >= args.end or "" in args.pages:
        parser.error(
            "La période doit inclure au moins deux jours et les pages doivent être renseignées"
        )
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]*", args.version):
        parser.error("Version de sortie invalide")
    if not re.fullmatch(r"[a-fA-F0-9]{64}", args.gold_export_sha256):
        parser.error("Empreinte SHA-256 de l'export invalide")
    output = f"{args.output_root.rstrip('/')}/{args.version}"
    spark = start_spark("TrendCast Wikimedia - dataset supervisé")
    try:
        ensure_new_hdfs_path(spark, output)
        build_dataset(spark, args, output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
