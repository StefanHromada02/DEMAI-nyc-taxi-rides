# kafka/producer.py
import os, json, time
import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pathlib import Path

BOOTSTRAP = os.getenv("BOOTSTRAP", "kafka:29092")  # <— so lassen!
TOPIC_Y, TOPIC_G = "taxi_yellow", "taxi_green"

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "parquet"
YELLOW = DATA_DIR / "yellow_tripdata_2025-01.parquet"
GREEN  = DATA_DIR / "green_tripdata_2025-01.parquet"

p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "linger.ms": 50,
    "batch.num.messages": 10000,
    "message.timeout.ms": 10000
})

def ensure_topics(bootstrap, topics):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = [NewTopic(t, num_partitions=1, replication_factor=1)
                 for t in topics if t not in existing]
    if to_create:
        fs = admin.create_topics(to_create)
        for t, f in fs.items():
            try:
                f.result()
                print(f"Topic created: {t}")
            except Exception as e:
                print(f"Topic create failed for {t}: {e}")

def load_norm(path: Path, service: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    if "tpep_pickup_datetime" in df:
        df = df.rename(columns={"tpep_pickup_datetime":"pickup_datetime",
                                "tpep_dropoff_datetime":"dropoff_datetime"})
    if "lpep_pickup_datetime" in df:
        df = df.rename(columns={"lpep_pickup_datetime":"pickup_datetime",
                                "lpep_dropoff_datetime":"dropoff_datetime"})
    ren = {"PULocationID":"pu_loc","DOLocationID":"do_loc","VendorID":"vendor_id"}
    for k,v in ren.items():
        if k in df.columns: df = df.rename(columns={k:v})
    keep = [c for c in [
        "pickup_datetime","dropoff_datetime","trip_distance",
        "fare_amount","tip_amount","total_amount","pu_loc","do_loc"
    ] if c in df.columns]
    df = df[keep].copy()
    df["service_type"] = service
    for c in ["pickup_datetime","dropoff_datetime"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

def send_df(df: pd.DataFrame, topic: str):
    sent = 0
    for _, r in df.iterrows():
        p.produce(topic, json.dumps(r.to_dict(), default=str).encode("utf-8"))
        p.poll(0)
        sent += 1
        if sent % 1000 == 0:
            print(f"{topic}: {sent} gesendet…")
        # time.sleep(0.001)  # optional drosseln
    p.flush()

def main():
    ensure_topics(BOOTSTRAP, [TOPIC_Y, TOPIC_G])
    if not YELLOW.exists() or not GREEN.exists():
        raise SystemExit("Parquet-Dateien nicht gefunden unter data/parquet/")
    y = load_norm(YELLOW, "yellow").head(10000)
    g = load_norm(GREEN,  "green").head(10000)
    print("send yellow:", len(y)); send_df(y, TOPIC_Y)
    print("send green :", len(g)); send_df(g, TOPIC_G)
    print("Fertig.")

if __name__ == "__main__":
    main()
