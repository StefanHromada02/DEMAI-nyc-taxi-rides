import os, re, json, time
from pathlib import Path
from typing import Iterable, Tuple

import pandas as pd
import pyarrow.parquet as pq
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from typing import Iterator, List, Dict
import heapq

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20000"))
PRELOAD_BATCHES = int(os.getenv("PRELOAD_BATCHES", "2"))  # wie viele Batches je Service gleichzeitig im Merge sein dürfen

BOOTSTRAP   = os.getenv("BOOTSTRAP", "kafka:29092")
TOPIC_Y     = os.getenv("TOPIC_Y", "taxi_yellow")
TOPIC_G     = os.getenv("TOPIC_G", "taxi_green")
DATA_DIR    = Path(os.getenv("DATA_DIR", Path(__file__).resolve().parents[1] / "data" / "parquet"))

# Livestream-Feeling: Nachrichten/Sekunde
RATE_MSGS_PER_SEC = int(os.getenv("RATE", "100"))       # z.B. 100 = ~10ms Abstand

# optional: ab Jahr starten (z.B. nur ab 2024)
START_YEAR = int(os.getenv("START_YEAR", "0"))          # 0 = ignorieren

FILE_RE = re.compile(r"^(yellow|green)_tripdata_(\d{4})-(\d{2})\.parquet$", re.IGNORECASE)

def create_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP,
        "linger.ms": 5,
        "batch.size": 131072,            # ~128 KB
        "compression.type": "zstd",
        "acks": "1",
        "enable.idempotence": False,
        "max.in.flight.requests.per.connection": 5,
        "message.timeout.ms": 30000,
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
    out.sort(key=lambda t: (t[0], t[1], 0 if t[2] == "yellow" else 1))
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
    Vereinheitlicht Pickup-Zeitspalte.
    Yellow: tpep_pickup_datetime, Green: lpep_pickup_datetime.
    Fallback: pickup_datetime, pickup.
    """
    def pick(*names):
        for n in names:
            if n in df.columns:
                return n
        return None

    col = pick("tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime", "pickup")
    if not col:
        raise ValueError("Keine Pickup-Datetime-Spalte gefunden.")
    df = df.copy()
    df["__pickup_ts"] = pd.to_datetime(df[col], errors="coerce", utc=False)
    df = df.dropna(subset=["__pickup_ts"])
    df = df.sort_values("__pickup_ts")
    return df

def drip_send_rows(producer: Producer, topic: str, records: Iterable[dict], rate: int):
    interval = None if rate <= 0 else 1.0 / rate
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
        producer.poll(0)

        if interval is not None:
            next_tick += interval
            sleep = next_tick - time.perf_counter()
            if sleep > 0:
                time.sleep(sleep)
            else:
                next_tick = time.perf_counter()
    return sent

def normalize_trip_type(df: pd.DataFrame) -> pd.DataFrame:
    if "trip_type" in df.columns:
        df["trip_type"] = pd.to_numeric(df["trip_type"], errors="coerce").astype("Int64")
    return df

def stream_month_by_day(path: Path, service: str, topic: str, rate: int, producer: Producer):
    """
    Speicher-schonend: liest Parquet in Batches und sendet innerhalb jedes Batches
    chronologisch nach __pickup_ts (keine Tages-Pausen mehr).
    """
    print(f"[{topic}] Lade (streaming) {path.name} …")
    pf = pq.ParquetFile(path)
    total_sent = 0

    for batch in pf.iter_batches(batch_size=20_000):
        df = batch.to_pandas()
        df["service_type"] = service
        df = normalize_datetimes_to_string(df)
        df = normalize_trip_type(df)

        # Pickup-Zeit vereinheitlichen + chronologisch sortieren
        df = add_pickup_column(df)  # liefert __pickup_ts
        records = df.drop(columns=["__pickup_ts"]).to_dict(orient="records")

        sent = drip_send_rows(producer, topic, records, rate)
        total_sent += sent
        print(f"[{topic}] {path.name}: +{sent} (Σ {total_sent})")

    producer.flush()
    print(f"[{topic}] {path.name}: total {total_sent} gesendet (streaming).")

def load_sorted_batch(path: Path, service: str, start_rowgroup: int) -> Tuple[int, pd.DataFrame] | None:
    """
    Lädt genau EIN Batch (ab start_rowgroup), normalisiert & sortiert nach __pickup_ts.
    Gibt (next_rowgroup_index, df_sorted) zurück oder None wenn keine Batches mehr.
    """
    pf = pq.ParquetFile(path)
    read = 0
    dfs: List[pd.DataFrame] = []
    for batch in pf.iter_batches(batch_size=BATCH_SIZE):
        if read < start_rowgroup:
            read += 1
            continue
        df = batch.to_pandas()
        df["service_type"] = service
        df = normalize_datetimes_to_string(df)
        df = normalize_trip_type(df)
        df = add_pickup_column(df)  # __pickup_ts
        dfs.append(df)
        read += 1
        break  # nur 1 Batch laden

    if not dfs:
        return None

    df_all = pd.concat(dfs, ignore_index=True)
    df_all.sort_values("__pickup_ts", inplace=True)
    return (read, df_all)

def iter_records_chronological_two_files(y_path: Path | None, g_path: Path | None) -> Iterator[dict]:
    """
    Liefert Records aus (yellow, green) global chronologisch.
    Nutzt pro Service bis zu PRELOAD_BATCHES parallele, sortierte Teil-DataFrames.
    """
    services = {
        "yellow": {"path": y_path, "next_idx": 0, "buffers": []},
        "green":  {"path": g_path, "next_idx": 0, "buffers": []},
    }

    heap: List[Tuple[pd.Timestamp, str, int, int]] = []

    def preload(service_key: str):
        svc = services[service_key]
        pth = svc["path"]
        if pth is None:
            return
        while len(svc["buffers"]) < PRELOAD_BATCHES:
            nxt = load_sorted_batch(pth, service_key, svc["next_idx"])
            if nxt is None:
                break
            svc["next_idx"], df_sorted = nxt
            if df_sorted.empty:
                continue
            buf_idx = len(svc["buffers"])
            svc["buffers"].append({"df": df_sorted, "pos": 0})
            ts0 = pd.to_datetime(df_sorted.iloc[0]["__pickup_ts"])
            heapq.heappush(heap, (ts0, service_key, buf_idx, 0))

    preload("yellow")
    preload("green")

    while heap:
        ts, skey, bidx, pos = heapq.heappop(heap)
        buf = services[skey]["buffers"][bidx]
        df = buf["df"]
        row = df.iloc[pos].to_dict()
        row.pop("__pickup_ts", None)
        yield row

        pos += 1
        if pos < len(df):
            nxt_ts = pd.to_datetime(df.iloc[pos]["__pickup_ts"])
            heapq.heappush(heap, (nxt_ts, skey, bidx, pos))
            buf["pos"] = pos
        else:
            services[skey]["buffers"][bidx] = None
            services[skey]["buffers"] = [b for b in services[skey]["buffers"] if b is not None]
            preload(skey)

def stream_month_pair_chronological(y_file: Path | None, g_file: Path | None,
                                    prod_y: Producer, prod_g: Producer,
                                    rate: int):
    """
    Sendet einen Monats-Slot: wenn y & g vorhanden -> interleaved chrono;
    sonst die vorhandene Datei allein.
    """
    if y_file is None and g_file is None:
        return
    if y_file is None:
        # nur green
        for _ in stream_single_file(g_file, "green", TOPIC_G, rate, prod_g):
            pass
        return
    if g_file is None:
        # nur yellow
        for _ in stream_single_file(y_file, "yellow", TOPIC_Y, rate, prod_y):
            pass
        return

    # beide vorhanden: global chrono
    sent_y = sent_g = 0
    for rec in iter_records_chronological_two_files(y_file, g_file):
        if rec.get("service_type") == "yellow":
            sent_y += drip_send_rows(prod_y, TOPIC_Y, [rec], rate)
        else:
            sent_g += drip_send_rows(prod_g, TOPIC_G, [rec], rate)
    prod_y.flush(); prod_g.flush()
    print(f"[merge] {y_file.name} + {g_file.name} → sent yellow={sent_y}, green={sent_g}")

def stream_single_file(path: Path, service: str, topic: str, rate: int, producer: Producer):
    """Fallback: eine Datei allein, chronologisch innerhalb der Datei (batchweise sortiert)."""
    yield from (stream_month_by_day(path, service, topic, RATE_MSGS_PER_SEC, producer),)

def main():
    ensure_topics(BOOTSTRAP, [TOPIC_Y, TOPIC_G])
    files = find_month_files(DATA_DIR)
    if not files:
        raise SystemExit(f"Keine Parquet-Dateien in {DATA_DIR} gefunden.")

    by_ym: dict[Tuple[int,int], dict[str,Path]] = {}
    for y, m, svc, p in files:
        by_ym.setdefault((y, m), {})[svc] = p

    months_sorted = sorted(by_ym.keys())

    print("Sende chronologisch pro Monat (yellow+green gemerged, falls beide vorhanden):")
    for (y, m) in months_sorted:
        y_file = by_ym[(y, m)].get("yellow")
        g_file = by_ym[(y, m)].get("green")
        label = f"{y}-{m:02d}"
        if y_file and g_file:
            print(f"  {label}: merge {y_file.name}  +  {g_file.name}")
        elif y_file:
            print(f"  {label}: only {y_file.name}")
        else:
            print(f"  {label}: only {g_file.name}")

    prod_y = create_producer()
    prod_g = create_producer()

    for (y, m) in months_sorted:
        y_file = by_ym[(y, m)].get("yellow")
        g_file = by_ym[(y, m)].get("green")
        stream_month_pair_chronological(y_file, g_file, prod_y, prod_g, RATE_MSGS_PER_SEC)

    print("Fertig: alle Monate verarbeitet.")

if __name__ == "__main__":
    main()
