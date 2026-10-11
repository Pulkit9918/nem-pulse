import io
import json
import logging
import os
import re
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests
from confluent_kafka import Producer

from .parser import nem_to_utc_iso, parse_mms_csv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nem-poller")

BASE = os.getenv("NEMWEB_BASE", "https://nemweb.com.au/Reports/Current/DispatchIS_Reports/")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
INITIAL_BACKFILL = int(os.getenv("INITIAL_BACKFILL", "12"))
STATE_PATH = Path(os.getenv("STATE_PATH", "data/state.json"))

FILE_RE = re.compile(r"PUBLIC_DISPATCHIS_\d{12}_\d+\.zip", re.IGNORECASE)

TOPICS = {
    "DISPATCH.PRICE": "nem.dispatch.price",
    "DISPATCH.REGIONSUM": "nem.dispatch.regionsum",
    "DISPATCH.INTERCONNECTORRES": "nem.dispatch.interconnector",
}


def load_state() -> list[str]:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())["processed"]
    return []


def save_state(processed: list[str]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps({"processed": processed[-500:]}))


def list_files(session: requests.Session) -> list[str]:
    r = session.get(BASE, timeout=30)
    r.raise_for_status()
    # Fixed-width timestamps in the name, so lexical sort == chronological sort
    return sorted({m.group(0) for m in FILE_RE.finditer(r.text)})


def delivery_cb(err, msg):
    if err is not None:
        log.error("delivery failed: %s", err)


def process_file(session: requests.Session, producer: Producer, name: str) -> int:
    r = session.get(BASE + name, timeout=60)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        text = z.read(z.namelist()[0]).decode("utf-8", errors="replace")

    ingested_at = datetime.now(timezone.utc).isoformat()
    count = 0
    for table, rec in parse_mms_csv(text):
        topic = TOPICS.get(table)
        if topic is None:
            continue
        ts = rec.get("SETTLEMENTDATE", "")
        entity = rec.get("REGIONID") or rec.get("INTERCONNECTORID", "")
        msg = {
            "table": table,
            "source_file": name,
            "ingested_at": ingested_at,
            "settlement_nem": ts,
            "settlement_utc": nem_to_utc_iso(ts) if ts else None,
            "data": rec,
        }
        producer.produce(topic, key=f"{entity}|{ts}", value=json.dumps(msg), on_delivery=delivery_cb)
        count += 1
    producer.flush(30)
    return count


def main() -> None:
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,
        "linger.ms": 50,
    })
    session = requests.Session()
    session.headers["User-Agent"] = "nem-pulse/0.1 (personal portfolio project)"

    processed = load_state()
    log.info("starting; %d files already processed", len(processed))

    while True:
        try:
            names = list_files(session)
            if not processed:
                todo = names[-INITIAL_BACKFILL:]
            else:
                seen = set(processed)
                todo = [n for n in names if n not in seen]
            for name in todo:
                n = process_file(session, producer, name)
                processed.append(name)
                save_state(processed)
                log.info("published %s (%d records)", name, n)
        except Exception:
            log.exception("poll cycle failed; will retry")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()