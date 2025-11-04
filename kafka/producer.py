import os, re, json, time, heapq
from pathlib import Path
from typing import Iterable, Tuple, Iterator, List, Dict, Optional

import pandas as pd
import pyarrow.parquet as pq
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# --------- Tunables (env) ---------
BATCH_SIZE       = int(os.getenv("BATCH_SIZE", "20000"))            # Arrow row-group batch
PRELOAD_BATCHES  = int(os.getenv("PRELOAD_BATCHES", "2"))           # preloaded sorted buffers per service
BOOTSTRAP        = os.getenv("BOOTSTRAP", "kafka:29092")
TOPIC_Y          = os.getenv("TOPIC_Y", "taxi_yellow")
TOPIC_G          = os.getenv("TOPIC_G", "taxi_green")
DATA_DIR         = Path(os.getenv("DATA_DIR", Path(__file__).resolve().parents[1] / "data" / "parquet"))
RATE_MSGS_PER_SEC= int(os.getenv("RATE", "100"))                    # 0 = as fast as possible
START_YEAR       = int(os.getenv("START_YEAR", "0"))                 # 0 = ignore
KEY_STRATEGY     = os.getenv("KEY_STRATEGY", "none").lower()         # none|service|pickup_day

# Send only what Spark actually reads (keeps payload tiny)
Y_KEEP = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "PULocationID", "DOLocationID",
    "passenger_count", "payment_type"
]
G_KEEP = [
    "lpep_pickup_datetime", "lpep_dropoff_datetime",
    "PULocationID", "DOLocationID",
    "passenger_count", "trip_type", "payment_type"
]

FILE_RE = re.compile(r"^(yellow|green)_tripdata_(\d{4})-(\d{2})\.parquet$", re.IGNORECASE)
json_dumps = json.dumps  # micro-optim

def create_producer() -> Producer:
    # Small linger + zstd already good; acks=1 keeps CPU down on broker+client
    # Avoid idempotence here (not needed for replayable offline seed; cheaper)
    return Producer({
        "bootstrap.servers": BOOTSTRAP,
        "linger.ms": 5,
        "batch.size": 131072,          # ~128 KiB per batch
        "compression.type": "zstd",
        "acks": "1",
        "enable.idempotence": False,
        "max.in.flight.requests.per.connection": 5,
        "message.timeout.ms": 30000,
        # Slightly reduce client memory footprint
        "queue.buffering.max.kbytes": 1024 * 64,   # 64 MiB
    })

def ensure_topics(bootstrap: str, topics: List[str]) -> None:
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

def find_month_files(base: Path) -> List[Tuple[int,int,str,Path]]:
    out: List[Tuple[int,int,str,Path]] = []
    for p in base.glob("*.parquet"):
        m = FILE_RE.match(p.name)
        if not m:
            continue
        service, y, mth = m.group(1).lower(), int(m.group(2)), int(m.group(3))
        if START_YEAR and y < START_YEAR:
            continue
        out.append((y, mth, service, p))
    # chronological with yellow before green for same (y,m)
    out.sort(key=lambda t: (t[0], t[1], 0 if t[2] == "yellow" else 1))
    return out

def _pickup_cols(service: str) -> Tuple[str, str]:
    # Map service -> pickup, dropoff column names
    if service == "yellow":
        return "tpep_pickup_datetime", "tpep_dropoff_datetime"
    return "lpep_pickup_datetime", "lpep_dropoff_datetime"

def _keep_cols(service: str) -> List[str]:
    return (Y_KEEP if service == "yellow" else G_KEEP)

def _project_iter_batches(pf: pq.ParquetFile, service: str, batch_size: int):
    # Project only needed columns directly at read-time (saves IO+RAM)
    cols = _keep_cols(service)
    for batch in pf.iter_batches(columns=cols, batch_size=batch_size):
        yield batch

def normalize_pick_drop(df: pd.DataFrame, service: str) -> pd.DataFrame:
    # Touch only pickup/dropoff cols (don’t scan all *_datetime)
    pcol, dcol = _pickup_cols(service)
    df = df.copy()
    if pcol in df.columns:
        df[pcol] = pd.to_datetime(df[pcol], errors="coerce", utc=False).astype("datetime64[ns]").astype(str)
    if dcol in df.columns:
        df[dcol] = pd.to_datetime(df[dcol], errors="coerce", utc=False).astype("datetime64[ns]").astype(str)
    return df

def normalize_trip_type(df: pd.DataFrame) -> pd.DataFrame:
    if "trip_type" in df.columns:
        df["trip_type"] = pd.to_numeric(df["trip_type"], errors="coerce").astype("Int64")
    return df

def add_pickup_sort_key(df: pd.DataFrame, service: str) -> pd.DataFrame:
    # Build __pickup_ts strictly from the relevant pickup col
    pcol, _ = _pickup_cols(service)
    if pcol not in df.columns:
        raise ValueError("Keine Pickup-Datetime-Spalte gefunden.")
    df = df.copy()
    df["__pickup_ts"] = pd.to_datetime(df[pcol], errors="coerce", utc=False)
    df = df.dropna(subset=["__pickup_ts"]).sort_values("__pickup_ts")
    return df

def _make_key(rec: dict) -> Optional[bytes]:
    # none: let Kafka round-robin across partitions → best utilization
    # service: keep two keys (yellow/green) → 2 partitions hot
    # pickup_day: spread by pickup date → moderate skew reduction without state
    if KEY_STRATEGY == "none":
        return None
    if KEY_STRATEGY == "service":
        return str(rec.get("service_type", "")).encode("utf-8")
    if KEY_STRATEGY == "pickup_day":
        # 2025-01-06 ... → “2025-01-06”
        for k in ("tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"):
            v = rec.get(k)
            if v:
                return v[:10].encode("utf-8")
    return None

def drip_send_rows(producer: Producer, topic: str, records: Iterable[dict], rate: int) -> int:
    # Rate-limited produce loop with gentle backpressure via poll()
    interval = None if rate <= 0 else 1.0 / rate
    next_tick = time.perf_counter()
    sent = 0
    for rec in records:
        payload = json_dumps(rec, default=str).encode("utf-8")
        key = _make_key(rec)
        while True:
            try:
                producer.produce(topic, value=payload, key=key)
                break
            except BufferError:
                producer.poll(0.05)  # free internal queues
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

def stream_month_file(path: Path, service: str, topic: str, rate: int, producer: Producer):
    # Stream one file in chronological order, batch-by-batch, with column projection
    print(f"[{topic}] streaming {path.name} …")
    pf = pq.ParquetFile(path)
    total_sent = 0
    for batch in _project_iter_batches(pf, service, batch_size=BATCH_SIZE):
        df = batch.to_pandas()
        df["service_type"] = service
        df = normalize_trip_type(normalize_pick_drop(df, service))
        df = add_pickup_sort_key(df, service)              # adds __pickup_ts & sorts
        # Remove sort key and keep only allowed columns plus service_type
        cols = set(_keep_cols(service)) | {"service_type"}
        records = df.drop(columns=["__pickup_ts"]).loc[:, cols].to_dict(orient="records")
        total_sent += drip_send_rows(producer, topic, records, rate)
        print(f"[{topic}] {path.name}: Σ {total_sent}")
    producer.flush()
    print(f"[{topic}] {path.name}: total {total_sent}")

def load_sorted_batch(path: Path, service: str, start_rowgroup: int) -> Optional[Tuple[int, pd.DataFrame]]:
    # Used by the two-file merge; also projects columns early
    pf = pq.ParquetFile(path)
    read = 0
    for batch in _project_iter_batches(pf, service, batch_size=BATCH_SIZE):
        if read < start_rowgroup:
            read += 1
            continue
        df = batch.to_pandas()
        df["service_type"] = service
        df = normalize_trip_type(normalize_pick_drop(df, service))
        df = add_pickup_sort_key(df, service)
        return (read + 1, df.sort_values("__pickup_ts"))
    return None

def iter_records_chronological_two_files(y_path: Optional[Path], g_path: Optional[Path]) -> Iterator[dict]:
    # Merge-iterator over yellow+green in global chronological order:
    services = {
        "yellow": {"path": y_path, "next_idx": 0, "buffers": []},
        "green":  {"path": g_path, "next_idx": 0, "buffers": []},
    }
    heap: List[Tuple[pd.Timestamp, str, int, int]] = []

    def preload(skey: str):
        svc = services[skey]
        pth = svc["path"]
        if pth is None:
            return
        while len(svc["buffers"]) < PRELOAD_BATCHES:
            nxt = load_sorted_batch(pth, skey, svc["next_idx"])
            if nxt is None:
                break
            svc["next_idx"], df_sorted = nxt
            if df_sorted.empty:
                continue
            bidx = len(svc["buffers"])
            svc["buffers"].append({"df": df_sorted, "pos": 0})
            ts0 = pd.to_datetime(df_sorted.iloc[0]["__pickup_ts"])
            heapq.heappush(heap, (ts0, skey, bidx, 0))

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

def stream_month_pair_chronological(y_file: Optional[Path], g_file: Optional[Path],
                                    prod_y: Producer, prod_g: Producer, rate: int):
    # If both exist: interleave chronologically; otherwise stream single file
    if y_file is None and g_file is None:
        return
    if y_file is None:
        stream_month_file(g_file, "green", TOPIC_G, rate, prod_g)
        return
    if g_file is None:
        stream_month_file(y_file, "yellow", TOPIC_Y, rate, prod_y)
        return

    sent_y = sent_g = 0
    for rec in iter_records_chronological_two_files(y_file, g_file):
        if rec.get("service_type") == "yellow":
            sent_y += drip_send_rows(prod_y, TOPIC_Y, [rec], rate)
        else:
            sent_g += drip_send_rows(prod_g, TOPIC_G, [rec], rate)
    prod_y.flush(); prod_g.flush()
    print(f"[merge] {y_file.name} + {g_file.name} → sent yellow={sent_y}, green={sent_g}")

def main():
    ensure_topics(BOOTSTRAP, [TOPIC_Y, TOPIC_G])
    files = find_month_files(DATA_DIR)
    if not files:
        raise SystemExit(f"Keine Parquet-Dateien in {DATA_DIR} gefunden.")

    by_ym: Dict[Tuple[int,int], Dict[str,Path]] = {}
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
