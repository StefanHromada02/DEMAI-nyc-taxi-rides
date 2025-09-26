import os, re, json, time
from pathlib import Path
from typing import Iterable, Tuple

import pandas as pd
import pyarrow.parquet as pq
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP   = os.getenv("BOOTSTRAP", "kafka:29092")
TOPIC_Y     = os.getenv("TOPIC_Y", "taxi_yellow")
TOPIC_G     = os.getenv("TOPIC_G", "taxi_green")
DATA_DIR    = Path(os.getenv("DATA_DIR", Path(__file__).resolve().parents[1] / "data" / "parquet"))

# Livestream-Feeling: Nachrichten/Sekunde & Pause zwischen Tagen
RATE_MSGS_PER_SEC = int(os.getenv("RATE", "100"))       # z.B. 100 = ~10ms Abstand
DAY_GAP_SEC       = float(os.getenv("DAY_GAP_SEC", "2"))# kurze Pause nach jedem Tag

# optional: ab Jahr starten (z.B. nur ab 2024)
START_YEAR = int(os.getenv("START_YEAR", "0"))          # 0 = ignorieren

FILE_RE = re.compile(r"^(yellow|green)_tripdata_(\d{4})-(\d{2})\.parquet$", re.IGNORECASE)

def create_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP,
        "linger.ms": 0,                       # drip feed → kein Bündeln
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 1,
        "compression.type": "lz4",
        "acks": "all",
        "message.timeout.ms": 15000,
    })

def ensure_topics(bootstrap: str, topics: list[str]) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = [NewTopic(t, num_partitions=1, replication_factor=1) for t in topics if t not in existing]
    if to_create:
        for t, f in admin.create_topics(to_create).items():
            try:
                f.result()
                print(f"Topic created: {t}")
            except Exception as e:
                print(f"Topic create failed for {t}: {e}")

def find_month_files(base: Path) -> list[Tuple[int,int,str,Path]]:
    """
    Scannt den Ordner und liefert (year, month, service, path) – nur valide Dateien.
    """
    out = []
    for p in base.glob("*.parquet"):
        m = FILE_RE.match(p.name)
        if not m: 
            continue
        service, y, mth = m.group(1).lower(), int(m.group(2)), int(m.group(3))
        if START_YEAR and y < START_YEAR:
            continue
        out.append((y, mth, service, p))
    # Ältestes zuerst
    out.sort(key=lambda t: (t[0], t[1], 0 if t[2]=="yellow" else 1))
    return out

def normalize_datetimes_to_string(df: pd.DataFrame) -> pd.DataFrame:
    """
    Konvertiert alle *_datetime-Spalten in ISO-Strings (Producer sendet Strings).
    """
    for c in df.columns:
        lc = str(c).lower()
        if "datetime" in lc or lc.endswith("_at"):
            try:
                ts = pd.to_datetime(df[c], errors="coerce", utc=False)
                df[c] = ts.astype("datetime64[ns]").astype(str)
            except Exception:
                pass
    return df

def add_pickup_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vereinheitlicht pickup-Spalte für die Tageslogik.
    Yellow: tpep_pickup_datetime, Green: lpep_pickup_datetime.
    Fallback: pickup_datetime, pickup.
    """
    candidates = [c for c in df.columns]
    def pick(*names):
        for n in names:
            if n in df.columns:
                return n
        return None

    col = pick("tpep_pickup_datetime","lpep_pickup_datetime","pickup_datetime","pickup")
    if not col:
        raise ValueError("Keine Pickup-Datetime-Spalte gefunden.")
    df = df.copy()
    df["__pickup_ts"] = pd.to_datetime(df[col], errors="coerce", utc=False)
    df = df.dropna(subset=["__pickup_ts"])
    df = df.sort_values("__pickup_ts")
    df["__pickup_day"] = df["__pickup_ts"].dt.date  # Python date
    return df

def drip_send_rows(producer: Producer, topic: str, records: Iterable[dict], rate: int):
    """
    Sendet Records mit konstanter Rate (msgs/s). Ruft regelmäßig poll(), um IO zu pumpen.
    """
    interval = 1.0 / max(rate, 1)
    next_tick = time.perf_counter()
    sent = 0
    for rec in records:
        payload = json.dumps(rec, default=str).encode("utf-8")
        while True:
            try:
                producer.produce(topic, value=payload, key=str(rec.get("service_type","")))
                break
            except BufferError:
                producer.poll(0.05)
        sent += 1
        producer.poll(0)  # Delivery-Callbacks

        # pacing
        next_tick += interval
        sleep = next_tick - time.perf_counter()
        if sleep > 0:
            time.sleep(sleep)
        else:
            next_tick = time.perf_counter()

    return sent

def stream_month_by_day(path: Path, service: str, topic: str, rate: int, day_gap_sec: float, producer: Producer):
    """
    Speicher-schonend: liest Parquet in Batches und sendet innerhalb jedes Batches
    die Datensätze tageweise in aufsteigender Reihenfolge (drip feed).
    Kein Full-Load in den RAM.
    """
    print(f"[{topic}] Lade (streaming) {path.name} …")
    pf = pq.ParquetFile(path)
    last_day = None
    total_sent = 0

    for batch in pf.iter_batches(batch_size=20_000):  # ggf. 10_000 bei wenig RAM
        df = batch.to_pandas()
        df["service_type"] = service
        df = normalize_datetimes_to_string(df)

        # Pickup-Zeitspalte vereinheitlichen (tpep/lpep/…)
        df = add_pickup_column(df)  # liefert __pickup_ts / __pickup_day

        # pro Batch tageweise senden (aufsteigend)
        for d, df_day in df.sort_values("__pickup_day").groupby("__pickup_day", sort=True):
            # Sleep nur beim Tagwechsel (nicht jedes Mal)
            if last_day is not None and d != last_day and day_gap_sec > 0:
                time.sleep(day_gap_sec)
            last_day = d

            records = df_day.drop(columns=["__pickup_day"]).to_dict(orient="records")
            sent = drip_send_rows(producer, topic, records, rate)
            total_sent += sent
            print(f"[{topic}] {path.name} – {d}: +{sent} (Σ {total_sent})")

    producer.flush()
    print(f"[{topic}] {path.name}: total {total_sent} gesendet (streaming).")


def main():
    ensure_topics(BOOTSTRAP, [TOPIC_Y, TOPIC_G])
    files = find_month_files(DATA_DIR)
    if not files:
        raise SystemExit(f"Keine Parquet-Dateien in {DATA_DIR} gefunden.")

    print("Gefundene Dateien (älteste zuerst):")
    for y, m, svc, p in files:
        print(f"  {svc:6s} {y}-{m:02d}  {p.name}")

    prod_y = create_producer()
    prod_g = create_producer()

    for (year, month, service, path) in files:
        topic = TOPIC_Y if service == "yellow" else TOPIC_G
        producer = prod_y if service == "yellow" else prod_g
        stream_month_by_day(path, service, topic, RATE_MSGS_PER_SEC, DAY_GAP_SEC, producer)

    print("Fertig (chronologisch, pro Tag).")

if __name__ == "__main__":
    main()
