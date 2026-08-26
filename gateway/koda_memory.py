from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any


class KodaMemory:
    """Local, evidence-only journal for live Koda setups.

    Records are created only from an unlocked gateway snapshot.  Outcomes are
    evaluated from later live prices; no simulated result is ever written.
    """

    def __init__(self, path: Path) -> None:
        self.connection = sqlite3.connect(path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS koda_setups (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_at TEXT NOT NULL,
              created_epoch REAL NOT NULL,
              direction TEXT NOT NULL,
              entry_price REAL NOT NULL,
              trigger_price REAL,
              tp1 REAL,
              tp2 REAL,
              invalidation REAL,
              confidence REAL,
              outcome TEXT NOT NULL DEFAULT 'WATCHING',
              outcome_price REAL,
              outcome_at TEXT,
              payload TEXT NOT NULL
            )
            """
        )
        self.connection.commit()
        self.last_signature: tuple[str, int] | None = None

    def close(self) -> None:
        self.connection.close()

    def observe(self, snapshot: dict[str, Any]) -> None:
        price = snapshot.get("instrument", {}).get("price")
        if not isinstance(price, (int, float)):
            return
        self._resolve_open_setups(float(price), str(snapshot.get("generatedAt") or ""))
        if not snapshot.get("ready"):
            return

        metrics = snapshot.get("metrics") or {}
        levels = snapshot.get("levels") or {}
        direction = str(metrics.get("bias") or "NEUTRAL")
        confidence = metrics.get("confidence")
        trigger = levels.get("trigger")
        invalidation = levels.get("invalidation")
        if direction not in {"BULLISH", "BEARISH"} or confidence is None or trigger is None or invalidation is None:
            return

        # One recorded setup per direction/price regime every two minutes.
        signature = (direction, round(float(trigger) * 4))
        bucket = int(time.time() // 120)
        if self.last_signature == (f"{signature[0]}:{signature[1]}", bucket):
            return
        latest = self.connection.execute(
            "SELECT created_epoch FROM koda_setups WHERE direction = ? AND outcome = 'WATCHING' ORDER BY id DESC LIMIT 1",
            (direction,),
        ).fetchone()
        if latest and time.time() - float(latest["created_epoch"]) < 120:
            return

        payload = {
            "metrics": metrics,
            "levels": levels,
            "equities": snapshot.get("equities"),
            "instrument": snapshot.get("instrument"),
            "marketStreams": snapshot.get("marketStreams"),
        }
        self.connection.execute(
            """
            INSERT INTO koda_setups
            (created_at, created_epoch, direction, entry_price, trigger_price, tp1, tp2, invalidation, confidence, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(snapshot.get("generatedAt") or ""),
                time.time(),
                direction,
                float(price),
                float(trigger),
                self._number(levels.get("tp1")),
                self._number(levels.get("tp2")),
                float(invalidation),
                float(confidence),
                json.dumps(payload, separators=(",", ":"), allow_nan=False),
            ),
        )
        self.connection.commit()
        self.last_signature = (f"{signature[0]}:{signature[1]}", bucket)

    def _resolve_open_setups(self, price: float, timestamp: str) -> None:
        rows = self.connection.execute(
            "SELECT id, direction, tp1, tp2, invalidation, created_epoch FROM koda_setups WHERE outcome = 'WATCHING'"
        ).fetchall()
        now = time.time()
        for row in rows:
            direction = row["direction"]
            target1, target2, stop = row["tp1"], row["tp2"], row["invalidation"]
            outcome: str | None = None
            if direction == "BULLISH":
                if stop is not None and price <= stop:
                    outcome = "INVALIDATED"
                elif target2 is not None and price >= target2:
                    outcome = "TP2 HIT"
                elif target1 is not None and price >= target1:
                    outcome = "TP1 HIT"
            elif direction == "BEARISH":
                if stop is not None and price >= stop:
                    outcome = "INVALIDATED"
                elif target2 is not None and price <= target2:
                    outcome = "TP2 HIT"
                elif target1 is not None and price <= target1:
                    outcome = "TP1 HIT"
            if outcome is None and now - float(row["created_epoch"]) >= 900:
                outcome = "EXPIRED"
            if outcome:
                self.connection.execute(
                    "UPDATE koda_setups SET outcome = ?, outcome_price = ?, outcome_at = ? WHERE id = ?",
                    (outcome, price, timestamp, int(row["id"])),
                )
        self.connection.commit()

    @staticmethod
    def _number(value: Any) -> float | None:
        return float(value) if isinstance(value, (int, float)) else None

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute(
            "SELECT * FROM koda_setups ORDER BY id DESC LIMIT 30"
        ).fetchall()
        setups = [
            {
                "id": int(row["id"]),
                "createdAt": row["created_at"],
                "direction": row["direction"],
                "entryPrice": row["entry_price"],
                "trigger": row["trigger_price"],
                "tp1": row["tp1"],
                "tp2": row["tp2"],
                "invalidation": row["invalidation"],
                "confidence": row["confidence"],
                "outcome": row["outcome"],
                "outcomePrice": row["outcome_price"],
                "outcomeAt": row["outcome_at"],
            }
            for row in rows
        ]
        resolved = [row for row in setups if row["outcome"] != "WATCHING"]
        winners = [row for row in resolved if row["outcome"] in {"TP1 HIT", "TP2 HIT"}]
        return {
            "total": len(setups),
            "resolved": len(resolved),
            "winRate": round((len(winners) / len(resolved)) * 100, 1) if resolved else None,
            "watching": sum(row["outcome"] == "WATCHING" for row in setups),
            "setups": setups,
        }
