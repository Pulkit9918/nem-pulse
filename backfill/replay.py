"""Replay staged MMSDM days into the live Kafka topics at an adjustable speed."""

import argparse
import json
import os
import time
from collections import Counter, defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from confluent_kafka import Producer

from backfill.stage import STAGED, TABLES

STATE = Path("data/backfill/replay_state.json")
INTERVAL_SECONDS = 300


def save_state(ts: str) -> None:
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps({"last_interval_utc": ts}))


def load_day(day: str) -> dict[str, list[dict]]:
    """All staged messages for one NEM calendar day, grouped by interval end (UTC)."""
    month_dir = STAGED / day[:7]
    by_interval = defaultdict(list)
    for table in TABLES:
        path = month_dir / table / f"{day}.jsonl"
        if not path.exists():
            continue
        with open(path, encoding="utf-8") as f:
            for line in f:
                msg = json.loads(line)
                by_interval[msg["settlement_utc"]].append(msg)
    return by_interval


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", required=True, help="first NEM day, YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="last NEM day, YYYY-MM-DD")
    ap.add_argument("--speed", type=float, default=1500.0,
                    help="multiple of real time (1500 = one day in about 58 s); 0 = no pacing")
    ap.add_argument("--dry-run", action="store_true", help="count messages, publish nothing")
    ap.add_argument("--resume", action="store_true", help="skip intervals already replayed")
    args = ap.parse_args()

    resume_after = ""
    if args.resume and STATE.exists():
        resume_after = json.loads(STATE.read_text())["last_interval_utc"]

    errors = Counter()

    def on_delivery(err, _msg):
        if err is not None:
            errors[str(err)] += 1

    producer = None if args.dry_run else Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:19092"),
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 20,
    })

    pause = INTERVAL_SECONDS / args.speed if args.speed > 0 else 0.0
    sent = Counter()
    next_tick = time.monotonic()
    day, last = date.fromisoformat(args.start), date.fromisoformat(args.end)

    while day <= last:
        by_interval = load_day(day.isoformat())
        if not by_interval:
            print(f"{day}: no staged data, skipping")
        day_total = 0
        for ts in sorted(by_interval):
            if ts <= resume_after:
                continue
            ingested_at = datetime.now(UTC).isoformat()
            for msg in by_interval[ts]:
                topic, key = msg.pop("topic"), msg.pop("msg_key")
                sent[topic] += 1
                day_total += 1
                if producer:
                    msg["ingested_at"] = ingested_at
                    producer.produce(topic, key=key, value=json.dumps(msg),
                                     on_delivery=on_delivery)
                    producer.poll(0)
            if producer:
                producer.flush(30)
                save_state(ts)
                next_tick += pause
                time.sleep(max(0.0, next_tick - time.monotonic()))
        print(f"{day}: {len(by_interval)} intervals, {day_total} messages")
        day += timedelta(days=1)

    label = "would publish" if args.dry_run else "published"
    print(f"{label} by topic: {dict(sent)}")
    if errors:
        print("DELIVERY ERRORS:", dict(errors))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())