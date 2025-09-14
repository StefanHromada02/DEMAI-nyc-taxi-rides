import json, time, pandas as pd
from confluent_kafka import Producer
from pathlib import Path

BOOTSTRAP = "localhost:9092"              # im Docker-Netz wäre es "kafka:9092"
TOPIC_Y, TOPIC_G = "taxi_yellow", "taxi_green"

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "parquet"
YELLOW = DATA_DIR / "yellow_tripdata_2025-01.parquet"
GREEN  = DATA_DIR / "green_tripdata_2025-01.parquet"

p = Producer({"bootstrap.servers": BOOTSTRAP})

def load_norm(path: Path, service: str) -> pd.DataFrame:
    df = pd.read_parquet(path)

    # Timestamps vereinheitlichen
    if "tpep_pickup_datetime" in df:
        df = df.rename(columns={"tpep_pickup_datetime":"pickup_datetime",
                                "tpep_dropoff_datetime":"dropoff_datetime"})
    if "lpep_pickup_datetime" in df:
        df = df.rename(columns={"lpep_pickup_datetime":"pickup_datetime",
                                "lpep_dropoff_datetime":"dropoff_datetime"})

    # IDs vereinheitlichen
    ren = {"PULocationID":"pu_loc","DOLocationID":"do_loc","VendorID":"vendor_id"}
    for k,v in ren.items():
        if k in df.columns: df = df.rename(columns={k:v})

    keep = [c for c in [
        "pickup_datetime","dropoff_datetime","trip_distance",
        "fare_amount","tip_amount","total_amount","pu_loc","do_loc"
    ] if c in df.columns]
    df = df[keep].copy()
    df["service_type"] = service

    # Datumsfelder parsen (robust)
    for c in ["pickup_datetime","dropoff_datetime"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

def send_df(df: pd.DataFrame, topic: str):
    for _, r in df.iterrows():
        p.produce(topic, json.dumps(r.to_dict(), default=str).encode("utf-8"))
        p.poll(0)
        time.sleep(0.001)  # „Realtime“-Simulation
    p.flush()

def main():
    if not YELLOW.exists() or not GREEN.exists():
        raise SystemExit("Parquet-Dateien nicht gefunden unter data/parquet/")
    y = load_norm(YELLOW, "yellow")
    g = load_norm(GREEN,  "green")
    print("send yellow:", len(y)); send_df(y, TOPIC_Y)
    print("send green :", len(g)); send_df(g, TOPIC_G)
    print("Fertig.")

if __name__ == "__main__":
    main()
