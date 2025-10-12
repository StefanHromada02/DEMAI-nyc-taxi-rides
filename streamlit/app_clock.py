# app.py — Live-Update mit Fake-Uhr (Simulated Now) + tz-fixes

import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo  # py≥3.9

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from streamlit_autorefresh import st_autorefresh  # pip install streamlit-autorefresh

st.set_page_config(page_title="Taxi Pipe – Dashboard", layout="wide")
st.title("End-to-End: Kafka → Spark → Postgres → Streamlit")

# -------------------- Config --------------------
DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://nyc:nyc@postgres:5432/nyc")
REFRESH_SEC = int(os.getenv("REFRESH_SEC", "10"))  # via docker-compose steuerbar

@st.cache_resource
def get_engine():
    return create_engine(DB_URL)

engine = get_engine()

# -------------------- Live-Update (Soft Re-Run) --------------------
with st.sidebar:
    st.header("Live-Update")
    auto = st.toggle("Auto-Refresh", value=True)
    interval = st.slider("Intervall (Sek.)", 2, 30, REFRESH_SEC)

run_id = 0
if auto:
    run_id = st_autorefresh(interval=interval * 1000, key="soft_refresh")

st.sidebar.caption(
    f"Aktualisiert alle {interval}s • Run #{run_id} • "
    f"{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
)

# Cache-Helper: TTL = Intervall
def _ttl():
    return max(2, interval)

@st.cache_data(ttl=_ttl(), show_spinner=False)
def fetch_df(sql: str, params=None):
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params)

# -------------------- Helpers --------------------
def base_union_sql(include_yellow: bool, include_green: bool) -> str:
    parts = []
    if include_yellow:
        parts.append("""
        SELECT
          id,
          'yellow'::text AS service_type,
          pickup_datetime, dropoff_datetime, trip_distance,
          fare_amount, tip_amount, total_amount,
          pu_loc, do_loc, vendor_id,
          NULL::int AS trip_type,
          passenger_count
        FROM public.rides_yellow
        """)
    if include_green:
        parts.append("""
        SELECT
          id,
          'green'::text AS service_type,
          pickup_datetime, dropoff_datetime, trip_distance,
          fare_amount, tip_amount, total_amount,
          pu_loc, do_loc, vendor_id,
          trip_type,
          passenger_count
        FROM public.rides_green
        """)
    return "\nUNION ALL\n".join(parts) if parts else "SELECT * FROM (SELECT NULL WHERE false) x"

def ensure_utc(ts):
    """Make any timestamp tz-aware in UTC (localize naive, convert aware)."""
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return None
    t = pd.to_datetime(ts, utc=True)  # if naive -> localize UTC; if aware -> convert to UTC
    return t

def fmt(ts, tz="UTC"):
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return "—"
    t = ensure_utc(ts).tz_convert(ZoneInfo(tz))
    return t.strftime("%Y-%m-%d %H:%M:%S %Z")

# -------------------- Fake-Clock (Simulated Now) --------------------
@st.cache_data(ttl=_ttl(), show_spinner=False)
def get_bounds_and_clock():
    union_sql = base_union_sql(True, True)
    sql = f"""
      WITH r AS ({union_sql})
      SELECT
        MIN(pickup_datetime) AS dmin,
        MAX(pickup_datetime) AS dmax,
        (SELECT MAX(created_at) FROM public.ingest_stats) AS last_batch_at
      FROM r
    """
    df = fetch_df(sql)
    dmin = df["dmin"].iloc[0]
    dmax = df["dmax"].iloc[0]
    last_batch = df["last_batch_at"].iloc[0]
    # normalize all to tz-aware UTC
    return ensure_utc(dmin), ensure_utc(dmax), ensure_utc(dmax), ensure_utc(last_batch)

first_all, last_all, sim_now_utc, last_batch_at_utc = get_bounds_and_clock()

# Simulated Now zentral anzeigen
cL, cMid, cR = st.columns([1,2,1])
with cMid:
    st.markdown(
        f"""
        <div style="text-align:center; line-height:1.2;">
          <div style="font-size:14px; color:#888;">Simulated Now</div>
          <div style="font-size:28px; font-weight:700;">{fmt(sim_now_utc, "UTC")}</div>
          <div style="font-size:13px; color:#888;">{fmt(sim_now_utc, "America/New_York")}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# Latenz (letzter Batch vs SimNow) – jetzt sicher, beide UTC-aware
latency_txt = "—"
if sim_now_utc is not None and last_batch_at_utc is not None:
    delta = sim_now_utc - last_batch_at_utc
    latency_txt = str(delta)

# -------------------- Sidebar-Filter --------------------
with st.sidebar:
    st.header("Filter")
    dmin = (first_all or pd.Timestamp("today", tz="UTC")).date()
    dmax = (last_all  or pd.Timestamp("today", tz="UTC")).date()
    start, end = st.date_input("Zeitraum", (dmin, dmax))
    services = st.multiselect("Service", ["yellow", "green"], default=["yellow", "green"])
    smooth = st.checkbox("Glätten (Rolling 3)", value=False)
    st.caption(f"Letzter Batch @ {fmt(last_batch_at_utc, 'UTC')} • Latenz vs. SimNow: {latency_txt}")
    if not services:
        st.stop()

incl_y = "yellow" in services
incl_g = "green"  in services
base_sql = base_union_sql(incl_y, incl_g)

params = {
    "start": pd.Timestamp(start, tz="UTC"),
    "end": pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1),
    "svc": services,  # psycopg2 wandelt Python-List -> PG-Array; ANY(:svc) funktioniert
}

# -------------------- KPIs --------------------
kpi_sql = f"""
WITH r AS (
{base_sql}
)
SELECT COUNT(*) AS rows,
       AVG(fare_amount)::numeric(10,2)   AS avg_fare,
       AVG(trip_distance)::numeric(10,2) AS avg_dist,
       SUM(total_amount)::numeric(12,2)  AS revenue
FROM r
WHERE pickup_datetime >= :start AND pickup_datetime < :end
  AND service_type = ANY(:svc)
"""
kpi = fetch_df(kpi_sql, params).iloc[0]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Datensätze in Postgres", f"{int(kpi['rows']):,}")
c2.metric("Ø Fare ($)",           kpi["avg_fare"])
c3.metric("Ø Distanz (mi)",       kpi["avg_dist"])
c4.metric("Umsatz ($)",           kpi["revenue"])

# -------------------- Yellow vs. Green --------------------
svc_sql = f"""
WITH r AS (
{base_sql}
)
SELECT service_type,
       COUNT(*) AS rows,
       AVG(fare_amount)::numeric(10,2)   AS avg_fare,
       AVG(trip_distance)::numeric(10,2) AS avg_dist
FROM r
WHERE pickup_datetime >= :start AND pickup_datetime < :end
  AND service_type = ANY(:svc)
GROUP BY service_type
ORDER BY service_type
"""
svc_df = fetch_df(svc_sql, params)
st.subheader("Yellow vs. Green")
st.dataframe(
    svc_df,
    width="stretch",
    column_config={
        "service_type": st.column_config.TextColumn("Service"),
        "rows":         st.column_config.NumberColumn("Zeilen", format="%,d"),
        "avg_fare":     st.column_config.NumberColumn("Ø Fare ($)", format="%.2f"),
        "avg_dist":     st.column_config.NumberColumn("Ø Distanz (mi)", format="%.2f"),
    },
)

# -------------------- Zeitreihe pro Stunde --------------------
ts_sql = f"""
WITH r AS (
{base_sql}
)
SELECT date_trunc('hour', pickup_datetime) AS hr,
       service_type, COUNT(*) AS rows
FROM r
WHERE pickup_datetime >= :start AND pickup_datetime < :end
  AND service_type = ANY(:svc)
GROUP BY hr, service_type
ORDER BY hr
"""
ts = fetch_df(ts_sql, params)
st.subheader("Fahrten pro Stunde")
if ts.empty:
    st.info("Keine Daten im gewählten Zeitraum/Filter.")
else:
    ts = ts.sort_values("hr")
    if smooth:
        ts["rows"] = ts.groupby("service_type")["rows"].transform(
            lambda s: s.rolling(3, min_periods=1).mean()
        )
    pivot = ts.pivot(index="hr", columns="service_type", values="rows")
    st.line_chart(pivot, width="stretch")

st.markdown("---")

# -------------------- Statisch: Hotspots & Anbieter --------------------
st.header("Statisch (mit Datumsauswahl)")

hot_sql = f"""
WITH r AS ({base_sql}),
base AS (
  SELECT
    r.pickup_datetime,
    COALESCE(zpu."Zone", 'ID '||r.pu_loc::text) AS pu_name,
    COALESCE(zdo."Zone", 'ID '||r.do_loc::text) AS do_name
  FROM r
  LEFT JOIN taxi_zones zpu ON zpu."LocationID" = r.pu_loc
  LEFT JOIN taxi_zones zdo ON zdo."LocationID" = r.do_loc
  WHERE r.pickup_datetime >= :start AND r.pickup_datetime < :end
    AND r.service_type = ANY(:svc)
)
SELECT
  date_trunc(:grain, pickup_datetime) AS period,
  COUNT(*) FILTER (WHERE pu_name IS NOT NULL) AS pu_count,
  COUNT(*) FILTER (WHERE do_name IS NOT NULL) AS do_count,
  pu_name,
  do_name
FROM base
GROUP BY period, pu_name, do_name
ORDER BY period;
"""
hot_m = fetch_df(hot_sql, {**params, "grain": "month"})
hot_y = fetch_df(hot_sql, {**params, "grain": "year"})

col_m, col_y = st.columns(2)
with col_m:
    st.subheader("Hotspots Start/Ende – monatlich")
    if hot_m.empty:
        st.info("Keine Daten.")
    else:
        top_pu = (hot_m.groupby(["period", "pu_name"], as_index=False)["pu_count"].sum()
                  .sort_values(["period", "pu_count"], ascending=[True, False])
                  .groupby("period").head(10))
        top_do = (hot_m.groupby(["period", "do_name"], as_index=False)["do_count"].sum()
                  .sort_values(["period", "do_count"], ascending=[True, False])
                  .groupby("period").head(10))
        st.write("Top Abholgebiete (PU)")
        st.dataframe(top_pu, width="stretch")
        st.write("Top Zielgebiete (DO)")
        st.dataframe(top_do, width="stretch")

with col_y:
    st.subheader("Hotspots Start/Ende – jährlich")
    if hot_y.empty:
        st.info("Keine Daten.")
    else:
        top_pu = (hot_y.groupby(["period", "pu_name"], as_index=False)["pu_count"].sum()
                  .sort_values(["period", "pu_count"], ascending=[True, False])
                  .groupby("period").head(10))
        top_do = (hot_y.groupby(["period", "do_name"], as_index=False)["do_count"].sum()
                  .sort_values(["period", "do_count"], ascending=[True, False])
                  .groupby("period").head(10))
        st.write("Top Abholgebiete (PU)")
        st.dataframe(top_pu, width="stretch")
        st.write("Top Zielgebiete (DO)")
        st.dataframe(top_do, width="stretch")

# Hail vs App
hail_sql = f"""
WITH r AS ({base_sql})
SELECT date_trunc(:grain, pickup_datetime) AS period,
       trip_type,
       COUNT(*) AS rows
FROM r
WHERE pickup_datetime >= :start AND pickup_datetime < :end
  AND service_type = ANY(:svc)
  AND trip_type IS NOT NULL
GROUP BY period, trip_type
ORDER BY period
"""
hail_m = fetch_df(hail_sql, {**params, "grain": "month"})
hail_y = fetch_df(hail_sql, {**params, "grain": "year"})

col_m2, col_y2 = st.columns(2)
with col_m2:
    st.subheader("Street-hail vs App – monatlich")
    if hail_m.empty:
        st.info("Keine Daten/keine trip_type-Angaben.")
    else:
        pv = hail_m.pivot(index="period", columns="trip_type", values="rows").fillna(0)
        pv.columns = ["street_hail(1)", "app(2)"] if set(pv.columns) == {1, 2} else [f"type_{c}" for c in pv.columns]
        st.bar_chart(pv, width="stretch")
with col_y2:
    st.subheader("Street-hail vs App – jährlich")
    if hail_y.empty:
        st.info("Keine Daten/keine trip_type-Angaben.")
    else:
        pv = hail_y.pivot(index="period", columns="trip_type", values="rows").fillna(0)
        pv.columns = ["street_hail(1)", "app(2)"] if set(pv.columns) == {1, 2} else [f"type_{c}" for c in pv.columns]
        st.bar_chart(pv, width="stretch")

# Vendor
vendor_sql = f"""
WITH r AS ({base_sql})
SELECT date_trunc(:grain, pickup_datetime) AS period,
       vendor_id, COUNT(*) AS rows
FROM r
WHERE pickup_datetime >= :start AND pickup_datetime < :end
  AND service_type = ANY(:svc)
  AND vendor_id IS NOT NULL
GROUP BY period, vendor_id
ORDER BY period, rows DESC
"""
vend_m = fetch_df(vendor_sql, {**params, "grain": "month"})
vend_y = fetch_df(vendor_sql, {**params, "grain": "year"})

col_m3, col_y3 = st.columns(2)
with col_m3:
    st.subheader("Anbieter (VendorID) – monatlich")
    if vend_m.empty:
        st.info("Keine Daten.")
    else:
        top_vendor = vend_m.groupby("period", as_index=False).apply(
            lambda g: g.nlargest(5, "rows")
        ).reset_index(drop=True)
        st.dataframe(top_vendor, width="stretch")
with col_y3:
    st.subheader("Anbieter (VendorID) – jährlich")
    if vend_y.empty:
        st.info("Keine Daten.")
    else:
        top_vendor = vend_y.groupby("period", as_index=False).apply(
            lambda g: g.nlargest(5, "rows")
        ).reset_index(drop=True)
        st.dataframe(top_vendor, width="stretch")
