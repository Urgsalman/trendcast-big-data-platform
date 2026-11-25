import sqlite3
from pathlib import Path

db = Path(".trendcast/ingestion.sqlite3")

if not db.exists():
    print("ERREUR : base SQLite introuvable :", db)
else:
    conn = sqlite3.connect(db)

    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()

    print("Tables :", tables)

    table_exists = conn.execute(
        """SELECT COUNT(*) FROM sqlite_master
           WHERE type='table' AND name='wikimedia_published_events'"""
    ).fetchone()[0]

    if table_exists:
        count = conn.execute(
            "SELECT COUNT(*) FROM wikimedia_published_events"
        ).fetchone()[0]
        print("IDs Wikimedia :", count)
    else:
        print("Table wikimedia_published_events introuvable")

    conn.close()
