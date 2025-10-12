# streamlit/app.py
import os
import pandas as pd
import numpy as np
import streamlit as st
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone, time as dtime

DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://nyc:nyc@postgres:5432/nyc")
REFRESH_SEC = int(os.getenv("REFRESH_SEC", "10"))

st.set_page_config(page_title="NYC Taxi Stream", layout="wide")

@st.cache_resource
def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True, pool_size=5, max_overflow=5)
engine = get_engine()

# -------------------- Queries (cached) --------------------
@st.cache_data(ttl=REFRESH_SEC)
def get_bounds():
    q = text("""
        WITH mins AS (
          SELECT MIN(pickup_datetime) AS ts FROM public.rides_yellow
          UNION ALL
          SELECT MIN(pickup_datetime) FROM public.rides_green
        ), maxs AS (
          SELECT MAX(pickup_datetime) AS ts FROM public.rides_yellow
          UNION ALL
          SELECT MAX(pickup_datetime) FROM public.rides_green
        )
        SELECT
          (SELECT MIN(ts) FROM mins) AS first_pickup,
          (SELECT MAX(ts) FROM maxs) AS last_pickup,
          (SELECT COALESCE(SUM(cnt),0) FROM (
            SELECT COUNT(*) AS cnt FROM public.rides_yellow
            UNION ALL
            SELECT COUNT(*) FROM public.rides_green
          ) s) AS total_rows,
          (SELECT MAX(created_at) FROM public.ingest_stats) AS last_batch_at
    """)
    with engine.connect() as cx:
        return cx.execute(q).mappings().first()

@st.cache_data(ttl=REFRESH_SEC)
def get_kpis():
    q = text("""
        SELECT
          (SELECT COUNT(*) FROM public.rides_yellow) AS yellow_rows,
          (SELECT COUNT(*) FROM public.rides_green)  AS green_rows,
          (SELECT COALESCE(SUM(rows_total),0) FROM public.ingest_stats WHERE service_type='yellow') AS yellow_ingested,
          (SELECT COALESCE(SUM(rows_total),0) FROM public.ingest_stats WHERE service_type='green')  AS green_ingested
    """)
    with engine.connect() as cx:
        return cx.execute(q).mappings().first()

@st.cache_data(ttl=REFRESH_SEC)
def get_timeseries(ts_from, ts_to, bucket="hour"):
    trunc = "hour" if bucket == "hour" else "day"
    q = text(f"""
      SELECT date_trunc('{trunc}', pickup_datetime) AS ts, 'yellow' AS svc, COUNT(*) AS n
      FROM public.rides_yellow
      WHERE pickup_datetime BETWEEN :a AND :b
      GROUP BY 1
      UNION ALL
      SELECT date_trunc('{trunc}', pickup_datetime) AS ts, 'green'  AS svc, COUNT(*) AS n
      FROM public.rides_green
      WHERE pickup_datetime BETWEEN :a AND :b
      GROUP BY 1
      ORDER BY 1, 2
    """)
    with engine.connect() as cx:
        return pd.read_sql(q, cx, params={"a": ts_from, "b": ts_to})

@st.cache_data(ttl=REFRESH_SEC)
def get_latest_batches(limit=50):
    q = text("""
      SELECT batch_id, service_type, rows_total, rows_null_pickup, rows_null_dropoff,
             rows_inversed, rows_equal_ts, rows_dupes, pickup_min, pickup_max, created_at
      FROM public.ingest_stats
      ORDER BY created_at DESC
      LIMIT :lim
    """)
    with engine.connect() as cx:
        return pd.read_sql(q, cx, params={"lim": int(limit)})

@st.cache_data(ttl=REFRESH_SEC)
def sample_rides(svc, ts_from, ts_to, limit=200):
    table = "public.rides_yellow" if svc == "yellow" else "public.rides_green"
    q = text(f"""
      SELECT pickup_datetime, dropoff_datetime, trip_distance,
             fare_amount, tip_amount, total_amount,
             pu_loc, do_loc, vendor_id
             {", trip_type" if svc=="green" else ""}
      FROM {table}
      WHERE pickup_datetime BETWEEN :a AND :b
      ORDER BY pickup_datetime ASC
      LIMIT :lim
    """)
    with engine.connect() as cx:
        return pd.read_sql(q, cx, params={"a": ts_from, "b": ts_to, "lim": int(limit)})

# -------------------- UI --------------------
st.title("🚕 NYC Taxi – Live Stream (Kafka → Spark → Postgres)")

bounds = get_bounds() or {}
kpis = get_kpis() or {}

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Yellow Rows", f"{(kpis.get('yellow_rows') or 0):,}")
col2.metric("Green Rows",  f"{(kpis.get('green_rows')  or 0):,}")
col3.metric("Total Rows",  f"{(bounds.get('total_rows') or 0):,}")
col4.metric("First Pickup (UTC)", str(bounds.get('first_pickup')) if bounds.get('first_pickup') else "—")
col5.metric("Last Batch @", str(bounds.get('last_batch_at')) if bounds.get('last_batch_at') else "—")

# ---- Filter: Date-Range (kompatibel statt datetime_input) ----
st.sidebar.header("Filter")
first = bounds.get('first_pickup')
last  = bounds.get('last_pickup')
default_from = (first if first else datetime.now(timezone.utc) - timedelta(days=7)).date()
default_to   = (last  if last  else datetime.now(timezone.utc)).date()

date_range = st.sidebar.date_input(
    "Zeitraum (UTC)",
    value=(default_from, default_to),
    help="Wähle Von/Bis als Datum. Zeit wird als 00:00–23:59:59 UTC angenommen."
)
# Streamlit gibt bei Single-Click evtl. nur ein Datum zurück → abfangen:
if isinstance(date_range, tuple):
    d_from, d_to = date_range
else:
    d_from, d_to = date_range, date_range

ts_from = datetime.combine(d_from, dtime.min, tzinfo=timezone.utc)
ts_to   = datetime.combine(d_to,   dtime.max, tzinfo=timezone.utc)

bucket  = st.sidebar.selectbox("Bucket", options=["hour", "day"], index=0)
limit   = st.sidebar.slider("Sample rows (Explorer)", min_value=50, max_value=2000, value=300, step=50)
svc     = st.sidebar.selectbox("Service (Explorer)", options=["yellow", "green"], index=0)

tab1, tab2, tab3 = st.tabs(["📈 Overview", "🔎 Explorer", "🧪 Batches"])

with tab1:
    st.subheader("Rides per " + ("Hour" if bucket == "hour" else "Day"))
    ts_df = get_timeseries(ts_from, ts_to, bucket=bucket)
    if ts_df.empty:
        st.info("Keine Daten im gewählten Zeitraum.")
    else:
        piv = ts_df.pivot(index="ts", columns="svc", values="n").fillna(0)
        st.line_chart(piv)

    st.markdown("**Letzte Batches (Top 15)**")
    st.dataframe(get_latest_batches(15), use_container_width=True)

with tab2:
    st.subheader(f"Sample rides • {svc.title()}")
    data = sample_rides(svc, ts_from, ts_to, limit=limit)
    st.dataframe(data, use_container_width=True, height=420)

with tab3:
    st.subheader("Batch Statistics (latest 50)")
    batches = get_latest_batches(50)
    st.dataframe(batches, use_container_width=True)

st.divider()
if st.button("🔄 Refresh jetzt"):
    st.cache_data.clear()
    st.rerun()
