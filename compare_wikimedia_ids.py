
import sqlite3
from pathlib import Path

DB_PATH = Path(".trendcast/ingestion.sqlite3")
HDFS_IDS_PATH = Path("wikimedia_hdfs_ids.txt")

with DB_PATH.open() as _:
    pass

with sqlite3.connect(DB_PATH) as connection:
    sqlite_ids = {
        row[0]
        for row in connection.execute(
            "SELECT event_id FROM wikimedia_published_events"
        )
    }

hdfs_ids = {
    line.strip().lstrip("\ufeff")
    for line in HDFS_IDS_PATH.read_text(
        encoding="utf-8-sig"
    ).splitlines()
    if line.strip()
}

only_sqlite = sqlite_ids - hdfs_ids
only_hdfs = hdfs_ids - sqlite_ids
common = sqlite_ids & hdfs_ids

print("========== WIKIMEDIA ID AUDIT ==========")
print("SQLite IDs:", len(sqlite_ids))
print("HDFS IDs:", len(hdfs_ids))
print("IDs communs:", len(common))
print("Présents uniquement dans SQLite:", len(only_sqlite))
print("Présents uniquement dans HDFS:", len(only_hdfs))

if only_sqlite:
    print("Exemples SQLite uniquement:", list(only_sqlite)[:5])

if only_hdfs:
    print("Exemples HDFS uniquement:", list(only_hdfs)[:5])

if sqlite_ids == hdfs_ids:
    print("RESULTAT: PERFECT MATCH")
else:
    print("RESULTAT: MISMATCH - investigate before ingestion")
