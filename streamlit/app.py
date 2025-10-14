# app.py — Live-Update mit laufender Fake-Uhr (Simulated Now), Pause/Play & Speed

import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo  # Python ≥3.9

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from streamlit_autorefresh import st_autorefresh
import altair as alt

st.set_page_config(page_title="Taxi Pipe – Dashboard", layout="wide")
st.title("End-to-End: Kafka → Spark → Postgres → Streamlit")

# -------------------- Config --------------------
DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://nyc:nyc@postgres:5432/nyc")
REFRESH_SEC = int(os.getenv("REFRESH_SEC", "10"))

@st.cache_resource
def get_engine():
    return create_engine(DB_URL)

engine = get_engine()

# -------------------- Live-Update (Soft Re-Run) --------------------
with st.sidebar:
    st.header("Live-Update")
    auto = st.toggle("Auto-Refresh", value=True)
    interval = st.slider("Intervall (Sek.)", 2, 30, REFRESH_SEC)
    if st.button("Cache leeren"):
        st.cache_data.clear()
        st.experimental_rerun()

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
def ensure_utc(ts):
    """tz-naive -> UTC lokalisiert; tz-aware -> nach UTC konvertiert"""
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return None
    return pd.to_datetime(ts, utc=True)

def fmt(ts, tz="UTC"):
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return "—"
    return ensure_utc(ts).tz_convert(ZoneInfo(tz)).strftime("%Y-%m-%d %H:%M:%S %Z")

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

@st.cache_data(ttl=_ttl(), show_spinner=False)
def get_first_last_and_lastbatch():
    sql = """
    WITH all_rides AS (
      SELECT pickup_datetime FROM public.rides_yellow
      UNION ALL
      SELECT pickup_datetime FROM public.rides_green
    )
    SELECT
      (SELECT MIN(pickup_datetime) FROM all_rides) AS first_pickup,
      (SELECT MAX(pickup_datetime) FROM all_rides) AS last_pickup,
      (SELECT MAX(created_at)      FROM public.ingest_stats) AS last_batch_at
    """
    df = fetch_df(sql)
    first_pick  = ensure_utc(df["first_pickup"].iloc[0])
    last_pick   = ensure_utc(df["last_pickup"].iloc[0])
    last_batch  = ensure_utc(df["last_batch_at"].iloc[0])
    return first_pick, last_pick, last_batch

# -------------------- Fake Time Simulation (laufend) --------------------
if "sim_anchor_real" not in st.session_state:
    st.session_state.sim_anchor_real = datetime.now(timezone.utc)
if "sim_anchor_fake" not in st.session_state:
    st.session_state.sim_anchor_fake = None
if "sim_speed" not in st.session_state:
    st.session_state.sim_speed = 1.0
if "sim_paused" not in st.session_state:
    st.session_state.sim_paused = False

first_all, last_all, last_batch_at = get_first_last_and_lastbatch()

if st.session_state.sim_anchor_fake is None:
    st.session_state.sim_anchor_fake = last_all or datetime.now(timezone.utc)

with st.sidebar:
    st.header("Simulation")
    colA, colB = st.columns(2)
    if colA.button("⏯ Pause/Play"):
        st.session_state.sim_paused = not st.session_state.sim_paused
        st.session_state.sim_anchor_real = datetime.now(timezone.utc)
    if colB.button("⟲ Reset → letzter Datensatz"):
        st.session_state.sim_anchor_fake = last_all or datetime.now(timezone.utc)
        st.session_state.sim_anchor_real = datetime.now(timezone.utc)
        st.session_state.sim_paused = False

    speed = st.selectbox("Geschwindigkeit", ["0.5×","1×","2×","10×","60×"], index=1)
    st.session_state.sim_speed = float(speed.replace("×",""))

now_utc = datetime.now(timezone.utc)
if st.session_state.sim_paused:
    sim_now_utc = st.session_state.sim_anchor_fake
else:
    elapsed_real = now_utc - st.session_state.sim_anchor_real
    sim_now_utc = st.session_state.sim_anchor_fake + elapsed_real * st.session_state.sim_speed

cL, cMid, cR = st.columns([1,2,1])
with cMid:
    st.markdown(
        f"""
        <div style="text-align:center; line-height:1.2; margin-top:6px;">
          <div style="font-size:14px; color:#888;">Simulated Now</div>
          <div style="font-size:28px; font-weight:700;">{fmt(sim_now_utc, "UTC")}</div>
          <div style="font-size:12px; color:#888;">{fmt(sim_now_utc, "America/New_York")}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

latency_txt = "—"
if sim_now_utc is not None and last_batch_at is not None:
    latency_txt = str(sim_now_utc - last_batch_at)

# -------------------- Sidebar-Filter --------------------
with st.sidebar:
    st.header("Filter")
    dmin = (first_all or pd.Timestamp("today", tz="UTC")).date()
    dmax = (last_all  or pd.Timestamp("today", tz="UTC")).date()
    start, end = st.date_input("Zeitraum", (dmin, dmax))
    services = st.multiselect("Service", ["yellow", "green"], default=["yellow", "green"])
    smooth = st.checkbox("Glätten (Rolling 3)", value=False)
    st.caption(f"Letzter Batch @ {fmt(last_batch_at, 'UTC')} • Latenz vs. SimNow: {latency_txt}")
    if not services:
        st.stop()

incl_y = "yellow" in services
incl_g = "green"  in services
base_sql = base_union_sql(incl_y, incl_g)

params = {
    "start": pd.Timestamp(start, tz="UTC"),
    "end": pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1),
    "svc": services,
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

st.subheader("Heatmap: Wochentag × Stunde")
if ts.empty:
    st.info("Keine Daten im gewählten Zeitraum/Filter.")
else:
    ts_sum = ts.groupby("hr", as_index=False)["rows"].sum()
    ts_sum["weekday"] = ts_sum["hr"].dt.weekday  # 0=Mo, 6=So
    ts_sum["hour"] = ts_sum["hr"].dt.hour
    wd_map = {0:"Mo",1:"Di",2:"Mi",3:"Do",4:"Fr",5:"Sa",6:"So"}
    ts_sum["wd_name"] = ts_sum["weekday"].map(wd_map)

    heat = alt.Chart(ts_sum).mark_rect().encode(
        x=alt.X("hour:O", title="Stunde"),
        y=alt.Y("wd_name:O", sort=["Mo","Di","Mi","Do","Fr","Sa","So"], title="Wochentag"),
        color=alt.Color("rows:Q", title="Fahrten"),
        tooltip=[alt.Tooltip("wd_name:N", title="Wochentag"),
                 alt.Tooltip("hour:O", title="Stunde"),
                 alt.Tooltip("rows:Q", title="Fahrten")]
    ).properties(width="container", height=240)
    st.altair_chart(heat, use_container_width=True)

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
  LEFT JOIN public.taxi_zones zpu ON zpu."LocationID" = r.pu_loc
  LEFT JOIN public.taxi_zones zdo ON zdo."LocationID" = r.do_loc
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
        st.write("Top Abholgebiete (PU)"); st.dataframe(top_pu, width="stretch")
        st.write("Top Zielgebiete (DO)");  st.dataframe(top_do, width="stretch")

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
        st.write("Top Abholgebiete (PU)"); st.dataframe(top_pu, width="stretch")
        st.write("Top Zielgebiete (DO)");  st.dataframe(top_do, width="stretch")

# Hail vs App
# Street-hail vs App (nur Green hat trip_type)
col_m2, col_y2 = st.columns(2)

def _hail_chart(grain: str):
    hail_sql = f"""
    SELECT
      date_trunc(:grain, pickup_datetime) AS period,
      CASE COALESCE(TRIM(trip_type::text),'')
        WHEN '1' THEN 'street_hail'
        WHEN '2' THEN 'app'
        ELSE 'unknown'
      END AS kind,
      COUNT(*) AS rows
    FROM public.rides_green
    WHERE pickup_datetime >= :start AND pickup_datetime < :end
      -- akzeptiert sowohl int als auch text:
      AND COALESCE(TRIM(trip_type::text),'') IN ('1','2')
    GROUP BY period, kind
    ORDER BY period;
    """
    return fetch_df(hail_sql, {**params, "grain": grain})


with col_m2:
    st.subheader("Street-hail vs App – monatlich")
    if "green" not in services:
        st.info("Nur für Green verfügbar. Wähle den Service „green“ aus.")
    else:
        hm = _hail_chart("month")
        if hm.empty:
            st.info("Keine Daten/keine trip_type-Angaben im Zeitraum.")
        else:
            pv = hm.pivot(index="period", columns="kind", values="rows").fillna(0)
            st.bar_chart(pv, width="stretch")

with col_y2:
    st.subheader("Street-hail vs App – jährlich")
    if "green" not in services:
        st.info("Nur für Green verfügbar. Wähle den Service „green“ aus.")
    else:
        hy = _hail_chart("year")
        if hy.empty:
            st.info("Keine Daten/keine trip_type-Angaben im Zeitraum.")
        else:
            pv = hy.pivot(index="period", columns="kind", values="rows").fillna(0)
            st.bar_chart(pv, width="stretch")


# Vendor (mit Namen)
# Vendor (mit Namen + ID, Top-5 je Periode in SQL)
vendor_sql = f"""
WITH r AS ({base_sql}),
agg AS (
  SELECT
    date_trunc(:grain, r.pickup_datetime)                          AS period,
    r.vendor_id,
    COALESCE(v.provider, 'Vendor '||r.vendor_id::text)             AS vendor_name,
    COUNT(*)                                                       AS rows
  FROM r
  LEFT JOIN public.vendor_providers v ON v.vendor_id = r.vendor_id
  WHERE r.pickup_datetime >= :start AND r.pickup_datetime < :end
    AND r.service_type = ANY(:svc)
    AND r.vendor_id IS NOT NULL
  GROUP BY period, r.vendor_id, vendor_name
),
ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY period ORDER BY rows DESC) AS rn
  FROM agg
)
SELECT period, vendor_name, rows
FROM ranked
WHERE rn <= 5
ORDER BY period, rows DESC;
"""

vend_m = fetch_df(vendor_sql, {**params, "grain": "month"})
vend_y = fetch_df(vendor_sql, {**params, "grain": "year"})

col_m3, col_y3 = st.columns(2)
with col_m3:
    st.subheader("Anbieter (VendorID) – monatlich")
    if vend_m.empty:
        st.info("Keine Daten.")
    else:
        # sorgt dafür, dass die ID-Spalte nicht „verschwindet“
        st.dataframe(
            vend_m, width="stretch",
            column_config={
                "period":      st.column_config.DatetimeColumn("Periode"),
                "vendor_name": st.column_config.TextColumn("Anbieter"),
                "rows":        st.column_config.NumberColumn("Fahrten", format="%,d"),
            },
        )

with col_y3:
    st.subheader("Anbieter (VendorID) – jährlich")
    if vend_y.empty:
        st.info("Keine Daten.")
    else:
        st.dataframe(
            vend_m, width="stretch",
            column_config={
                "period":      st.column_config.DatetimeColumn("Periode"),
                "vendor_name": st.column_config.TextColumn("Anbieter"),
                "rows":        st.column_config.NumberColumn("Fahrten", format="%,d"),
            },
        )


# (Optional) Diagnose: zeigt, welche vendor_id im Filterzeitraum vorkommt
diag_sql = f"""
WITH r AS ({base_sql})
SELECT r.vendor_id,
       COALESCE(v.provider, '— (kein Mapping)') AS provider,
       COUNT(*) AS rows
FROM r
LEFT JOIN public.vendor_providers v ON v.vendor_id = r.vendor_id
WHERE r.pickup_datetime >= :start AND r.pickup_datetime < :end
  AND r.service_type = ANY(:svc)
GROUP BY r.vendor_id, provider
ORDER BY rows DESC;
"""
with st.expander("Vendor-Diagnose (Debug)"):
    st.dataframe(fetch_df(diag_sql, params), use_container_width=True)

# -------------------- OD-Heatmap --------------------
st.subheader("Heatmap: Pickup-Zone × Dropoff-Zone (Top 20)")
od_sql = f"""
WITH r AS ({base_sql}),
base AS (
  SELECT
    COALESCE(zpu."Zone", 'ID '||r.pu_loc::text) AS pu_name,
    COALESCE(zdo."Zone", 'ID '||r.do_loc::text) AS do_name
  FROM r
  LEFT JOIN public.taxi_zones zpu ON zpu."LocationID" = r.pu_loc
  LEFT JOIN public.taxi_zones zdo ON zdo."LocationID" = r.do_loc
  WHERE r.pickup_datetime >= :start AND r.pickup_datetime < :end
    AND r.service_type = ANY(:svc)
)
SELECT pu_name, do_name, COUNT(*) AS trips
FROM base
GROUP BY pu_name, do_name
"""
od = fetch_df(od_sql, params)

if od.empty:
    st.info("Keine OD-Daten im gewählten Zeitraum/Filter.")
else:
    N = st.slider("Top-N Zonen für OD-Heatmap", 5, 40, 20)
    top_pu = (od.groupby("pu_name")["trips"].sum()
                .sort_values(ascending=False).head(N).index)
    top_do = (od.groupby("do_name")["trips"].sum()
                .sort_values(ascending=False).head(N).index)
    od_top = od[od["pu_name"].isin(top_pu) & od["do_name"].isin(top_do)]

    heat_od = alt.Chart(od_top).mark_rect().encode(
        x=alt.X("do_name:N", sort="-y", title="Dropoff-Zone"),
        y=alt.Y("pu_name:N", sort="-x", title="Pickup-Zone"),
        color=alt.Color("trips:Q", title="Fahrten"),
        tooltip=[alt.Tooltip("pu_name:N", title="Pickup"),
                 alt.Tooltip("do_name:N", title="Dropoff"),
                 alt.Tooltip("trips:Q", title="Fahrten")]
    ).properties(width="container", height=520)
    st.altair_chart(heat_od, use_container_width=True)
