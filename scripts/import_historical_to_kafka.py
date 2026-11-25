
import argparse
import logging
import os
import sqlite3
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Permet d'importer les modules du projet depuis scripts/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.producers.kafka_producer import KafkaEventProducer
from ingestion.validators.event_validator import validate_event

load_dotenv(PROJECT_ROOT / ".env")

logger = logging.getLogger("historical_import")

DATASET_PATH = (
    PROJECT_ROOT
    / "data"
    / "historical"
    / "wikipedia_pageviews_clean.parquet"
)

DEFAULT_STATE_PATH = PROJECT_ROOT / ".trendcast" / "ingestion.sqlite3"

SOURCE = "wikimedia_pageviews"


def get_state_path() -> Path:
    configured = os.getenv("INGESTION_STATE_PATH")
    if configured:
        path = Path(configured)
        return path if path.is_absolute() else PROJECT_ROOT / path
    return DEFAULT_STATE_PATH


def load_registry(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute(
        """
        SELECT event_id
        FROM wikimedia_published_events
        """
    ).fetchall()

    return {row[0] for row in rows}


def build_event(row: pd.Series, project: str) -> dict:
    article = str(row["article"]).strip()
    date = pd.Timestamp(row["date"])
    views = int(row["views"])

    if not article:
        raise ValueError("Article vide")

    if pd.isna(date):
        raise ValueError("Date invalide")

    if views < 0:
        raise ValueError("Nombre de vues négatif")

    date_id = date.strftime("%Y%m%d")
    event_id = f"{project}:{article}:{date_id}00"

    return {
        "event_id": event_id,
        "source": SOURCE,
        "timestamp": date.strftime("%Y-%m-%dT00:00:00Z"),
        "schema_version": "1.0",
        "payload": {
            "project": project,
            "page_title": article,
            "granularity": "daily",
            "views": views,
        },
    }


def save_event_id(
    connection: sqlite3.Connection,
    event_id: str,
) -> None:
    connection.execute(
        """
        INSERT OR IGNORE INTO wikimedia_published_events (event_id)
        VALUES (?)
        """,
        (event_id,),
    )
    connection.commit()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import historique Wikimedia vers Kafka."
    )

    parser.add_argument(
        "--publish",
        action="store_true",
        help="Publier réellement les événements dans Kafka.",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Nombre maximum d'événements à traiter.",
    )

    parser.add_argument(
        "--project",
        default=os.getenv("WIKIMEDIA_PROJECT", "en.wikipedia"),
        help="Projet Wikimedia.",
    )

    args = parser.parse_args()

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit doit être supérieur à zéro.")

    return args


def main():
    args = parse_args()

    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"Dataset introuvable : {DATASET_PATH}"
        )

    state_path = get_state_path()

    if not state_path.exists():
        raise FileNotFoundError(
            f"Registre SQLite introuvable : {state_path}"
        )

    with sqlite3.connect(state_path, timeout=30) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                """
            ).fetchall()
        }

        if "wikimedia_published_events" not in tables:
            raise RuntimeError(
                "Table wikimedia_published_events introuvable."
            )

        registry_ids = load_registry(connection)

        minimum_ids = int(
            os.getenv("WIKIMEDIA_DEDUP_MIN_IDS", "30971")
        )

        if len(registry_ids) < minimum_ids:
            raise RuntimeError(
                "Registre Wikimedia insuffisant : "
                f"{len(registry_ids)} IDs trouvés, "
                f"{minimum_ids} minimum requis."
            )

        logger.info(
            "Registre SQLite chargé : %s IDs",
            len(registry_ids),
        )

        df = pd.read_parquet(DATASET_PATH)

        required_columns = {"article", "views", "date"}
        missing_columns = required_columns - set(df.columns)

        if missing_columns:
            raise ValueError(
                f"Colonnes manquantes : {missing_columns}"
            )

        df["date"] = pd.to_datetime(
            df["date"],
            errors="coerce",
        )

        df["views"] = pd.to_numeric(
            df["views"],
            errors="coerce",
        )

        df = df.dropna(
            subset=["article", "date", "views"]
        ).copy()

        df["article"] = df["article"].astype(str).str.strip()
        df = df[df["article"] != ""]
        df = df[df["views"] >= 0]

        df = df.drop_duplicates(
            subset=["article", "date"]
        )

        df = df.sort_values(
            ["date", "article"]
        )

        project = args.project

        candidates = []
        already_registered = 0
        invalid = 0

        for _, row in df.iterrows():
            try:
                event = build_event(row, project)
                event = validate_event(event)

                if event["event_id"] in registry_ids:
                    already_registered += 1
                    continue

                candidates.append(event)

            except Exception:
                invalid += 1
                logger.exception(
                    "Événement invalide pour article=%s",
                    row.get("article"),
                )

        total_candidates = len(candidates)

        if args.limit is not None:
            candidates = candidates[:args.limit]

        print("\n========== IMPORT HISTORIQUE ==========")
        print(f"Lignes nettoyées       : {len(df)}")
        print(f"Déjà dans SQLite       : {already_registered}")
        print(f"Événements invalides   : {invalid}")
        print(f"Candidats disponibles  : {total_candidates}")
        print(f"Événements sélectionnés: {len(candidates)}")
        print(f"Mode publication       : {'ACTIF' if args.publish else 'SIMULATION'}")
        print("=======================================\n")

        if not args.publish:
            print(
                "Simulation terminée. Aucun événement envoyé à Kafka."
            )
            return

        if not candidates:
            print("Aucun événement à publier.")
            return

        producer = KafkaEventProducer()
        published = 0
        failed = 0

        try:
            for index, event in enumerate(candidates, start=1):
                try:
                    # publish() attend l'ACK du broker Kafka.
                    producer.publish(event)

                    # Enregistrement immédiatement après confirmation.
                    save_event_id(
                        connection,
                        event["event_id"],
                    )

                    registry_ids.add(event["event_id"])
                    published += 1

                    if index % 100 == 0:
                        print(
                            f"Progression : {index}/{len(candidates)} "
                            f"| publiés : {published}"
                        )

                except Exception:
                    failed += 1
                    logger.exception(
                        "Échec de publication : %s. "
                        "Arrêt pour éviter de continuer dans un état incertain.",
                        event["event_id"],
                    )
                    break

        finally:
            producer.close()

        print("\n========== RÉSULTAT ==========")
        print(f"Publiés et enregistrés : {published}")
        print(f"Échecs                 : {failed}")
        print("==============================")

        if failed:
            raise RuntimeError(
                "Import interrompu après une erreur. "
                "Relance le script : les IDs confirmés sont conservés."
            )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    main()
