
import sqlite3
from pathlib import Path

import pandas as pd

DB = Path(".trendcast/ingestion.sqlite3")
HDFS_IDS = Path("wikimedia_hdfs_ids.txt")
DATASET = Path("data/historical/wikipedia_pageviews_clean.parquet")

# Lire les identifiants du fichier exporté depuis HDFS
hdfs_ids = {
    line.strip()
    for line in HDFS_IDS.read_text(encoding="utf-8-sig").splitlines()
    if line.strip()
}

# Lire le registre SQLite sans le modifier
with sqlite3.connect(f"file:{DB.resolve()}?mode=ro", uri=True) as conn:
    registry_ids = {
        row[0]
        for row in conn.execute(
            "SELECT event_id FROM wikimedia_published_events"
        )
    }

# Construire les identifiants correspondant au dataset historique
df = pd.read_parquet(DATASET)

df["event_id"] = (
    "en.wikipedia:"
    + df["article"].astype(str)
    + ":"
    + df["date"].dt.strftime("%Y%m%d00")
)

dataset_ids = set(df["event_id"])

print("\n========== AUDIT DES IDENTIFIANTS ==========")
print("HDFS - lignes uniques :", len(hdfs_ids))
print("SQLite - IDs uniques :", len(registry_ids))
print("Dataset - lignes :", len(df))
print("Dataset - IDs uniques :", len(dataset_ids))

print("\n========== COMPARAISON HDFS / SQLITE ==========")
print("Communs :", len(hdfs_ids & registry_ids))
print("Uniquement dans SQLite :", len(registry_ids - hdfs_ids))
print("Uniquement dans HDFS :", len(hdfs_ids - registry_ids))

print("\n========== CHEVAUCHEMENT AVEC LE DATASET ==========")
print("Déjà dans HDFS :", len(dataset_ids & hdfs_ids))
print("Déjà dans SQLite :", len(dataset_ids & registry_ids))
print("Absents des deux :", len(dataset_ids - hdfs_ids - registry_ids))

print("\n========== EXEMPLES ==========")
print("SQLite seulement :", list(registry_ids - hdfs_ids)[:5])
print("HDFS seulement :", list(hdfs_ids - registry_ids)[:5])
print("Dataset déjà dans HDFS :", list(dataset_ids & hdfs_ids)[:5])

print("\n========== FIN DE L'AUDIT ==========")
