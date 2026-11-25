"""Persistent publication receipts, separate from Spark's future checkpoints."""

import sqlite3
from pathlib import Path


class PublicationState:
    def __init__(self, path: str):
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(target, timeout=30)
        self.connection.executescript(
            "CREATE TABLE IF NOT EXISTS receipts ("
            "publication_key TEXT PRIMARY KEY, confirmed_at TEXT NOT NULL "
            "DEFAULT CURRENT_TIMESTAMP);"
            "CREATE TABLE IF NOT EXISTS history_starts ("
            "series_key TEXT PRIMARY KEY, start_date TEXT NOT NULL);"
        )

    def contains(self, key: str) -> bool:
        return (
            self.connection.execute(
                "SELECT 1 FROM receipts WHERE publication_key = ?", (key,)
            ).fetchone()
            is not None
        )

    def confirm(self, key: str) -> None:
        # Called only after KafkaEventProducer.publish receives the broker ACK.
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO receipts(publication_key) VALUES (?)", (key,)
            )

    def history_start(self, series: str, initial: str) -> str:
        # Retain the original lower bound even after a prolonged interruption.
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO history_starts VALUES (?, ?)", (series, initial)
            )
        return self.connection.execute(
            "SELECT start_date FROM history_starts WHERE series_key = ?", (series,)
        ).fetchone()[0]

    def close(self) -> None:
        self.connection.close()
