# app.py — Live-Update ohne Seiten-Reload, ohne DB-View (UNION ALL in SQL)

import os
from datetime import datetime, timezone

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
    # Script-ReRun alle X Sekunden (kein Browser-Reload, kein Flackern)
    run_id = st_autorefresh(interval=interval * 1000, key="soft_refresh")

st.sidebar.caption(
    f"Aktualisiert alle {interval}s • Run #{run_id} • "
    f"{datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
)

# Cache-Helper: TTL = Intervall, damit jede Runde frische Daten holt
def _ttl():
    return max(2, interval)

@st.cache_data(ttl=_ttl(), show_spinner=False)
def fetch_df(sql: str, params=None):
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params)

# -------------------- UNION-Helper (ohne View) --------------------
def base_union_sql(include_yellow: bool, include_green: bool) -> str:
    """
    Baut ein UNION ALL über rides_yellow/green und liefert identisches Schema
    wie die alte 'rides'-Tabelle (inkl. literalem service_type).
    """
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
        FROM rides_yellow
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
        FROM rides_green
        """)
    # mindestens eine Source
    return "\nUNION ALL\n".join(parts) if parts else "SELECT * FROM (SELECT NULL WHERE false) x"

# -------------------- Sidebar-Filter --------------------
with st.sidebar:
    st.header("Filter")
    # Bounds brauchen UNION-Basis; initial beide Services annehmen
    base_sql_bounds = base_union_sql(True, True)
    bounds = fetch_df(f"WITH r AS ({base_sql_bounds}) SELECT min(pickup_datetime) AS dmin, max(pickup_datetime) AS dmax FROM r")
    dmin = (bounds["dmin"].iloc[0] or pd.Timestamp("today")).date()
    dmax = (bounds["dmax"].iloc[0] or pd.Timestamp("today")).date()
    start, end = st.date_input("Zeitraum", (dmin, dmax))
    services = st.multiselect("Service", ["yellow", "green"], default=["yellow", "green"])
    smooth = st.checkbox("Glätten (Rolling 3)", value=False)
    if not services:
        st.stop()

incl_y = "yellow" in services
incl_g = "green"  in services
base_sql = base_union_sql(incl_y, incl_g)

params = {
    "start": pd.Timestamp(start),
    "end": pd.Timestamp(end) + pd.Timedelta(days=1),
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

# Hotspots (mit Zonen-Namen)
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
        st.dataframe(
            top_pu,
            width="stretch",
            column_config={
                "period":   st.column_config.DatetimeColumn("Periode"),
                "pu_name":  st.column_config.TextColumn("Pickup-Zone"),
                "pu_count": st.column_config.NumberColumn("Fahrten", format="%,d"),
            },
        )
        st.write("Top Zielgebiete (DO)")
        st.dataframe(
            top_do,
            width="stretch",
            column_config={
                "period":   st.column_config.DatetimeColumn("Periode"),
                "do_name":  st.column_config.TextColumn("Dropoff-Zone"),
                "do_count": st.column_config.NumberColumn("Fahrten", format="%,d"),
            },
        )

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
        st.dataframe(
            top_pu,
            width="stretch",
            column_config={
                "period":   st.column_config.DatetimeColumn("Periode"),
                "pu_name":  st.column_config.TextColumn("Pickup-Zone"),
                "pu_count": st.column_config.NumberColumn("Fahrten", format="%,d"),
            },
        )
        st.write("Top Zielgebiete (DO)")
        st.dataframe(
            top_do,
            width="stretch",
            column_config={
                "period":   st.column_config.DatetimeColumn("Periode"),
                "do_name":  st.column_config.TextColumn("Dropoff-Zone"),
                "do_count": st.column_config.NumberColumn("Fahrten", format="%,d"),
            },
        )

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
        st.dataframe(
            top_vendor,
            width="stretch",
            column_config={
                "period":    st.column_config.DatetimeColumn("Periode"),
                "vendor_id": st.column_config.NumberColumn("VendorID", format="%,d"),
                "rows":      st.column_config.NumberColumn("Fahrten", format="%,d"),
            },
        )
with col_y3:
    st.subheader("Anbieter (VendorID) – jährlich")
    if vend_y.empty:
        st.info("Keine Daten.")
    else:
        top_vendor = vend_y.groupby("period", as_index=False).apply(
            lambda g: g.nlargest(5, "rows")
        ).reset_index(drop=True)
        st.dataframe(
            top_vendor,
            width="stretch",
            column_config={
                "period":    st.column_config.DatetimeColumn("Periode"),
                "vendor_id": st.column_config.NumberColumn("VendorID", format="%,d"),
                "rows":      st.column_config.NumberColumn("Fahrten", format="%,d"),
            },
        )

# -------------------- Dynamisch --------------------
st.header("Dynamisch (je nach Tag & Tageszeit)")

# Rush (Zonen-Namen)
rush_sql = f"""
WITH r AS ({base_sql})
SELECT
  EXTRACT(ISODOW FROM r.pickup_datetime)::int AS weekday,  -- 1=Mo ... 7=So
  EXTRACT(HOUR   FROM r.pickup_datetime)::int AS hour,
  COALESCE(zpu."Zone", 'ID '||r.pu_loc::text) AS pu_name,
  COUNT(*) AS rides
FROM r
LEFT JOIN taxi_zones zpu ON zpu."LocationID" = r.pu_loc
WHERE r.pickup_datetime >= :start AND r.pickup_datetime < :end
  AND r.service_type = ANY(:svc)
GROUP BY weekday, hour, pu_name
ORDER BY weekday, hour, rides DESC
"""
rush = fetch_df(rush_sql, params)
if rush.empty:
    st.info("Keine Daten.")
else:
    top_rush = rush.groupby(["weekday", "hour"], as_index=False).apply(
        lambda g: g.nlargest(5, "rides")
    ).reset_index(drop=True)
    st.dataframe(
        top_rush,
        width="stretch",
        column_config={
            "weekday": st.column_config.NumberColumn("Wochentag", format="%d"),
            "hour":    st.column_config.NumberColumn("Stunde", format="%02d"),
            "pu_name": st.column_config.TextColumn("Pickup-Zone"),
            "rides":   st.column_config.NumberColumn("Fahrten", format="%,d"),
        },
    )

# Tip-Rate (Qualitätsfilter)
tip_sql = f"""
WITH r AS ({base_sql})
SELECT
  EXTRACT(ISODOW FROM r.pickup_datetime)::int AS weekday,
  COALESCE(zpu."Zone", 'ID '||r.pu_loc::text) AS pu_name,
  AVG(
    CASE
      WHEN r.fare_amount >= 5
       AND r.tip_amount >= 0
       AND r.tip_amount <= r.fare_amount
      THEN r.tip_amount / r.fare_amount
      ELSE NULL
    END
  ) * 100.0 AS tip_pct
FROM r
LEFT JOIN taxi_zones zpu ON zpu."LocationID" = r.pu_loc
WHERE r.pickup_datetime >= :start AND r.pickup_datetime < :end
  AND r.service_type = ANY(:svc)
GROUP BY weekday, pu_name
ORDER BY weekday, tip_pct DESC
"""
tip = fetch_df(tip_sql, params)
if tip.empty:
    st.info("Keine Daten.")
else:
    st.dataframe(
        tip,
        width="stretch",
        column_config={
            "weekday": st.column_config.NumberColumn("Wochentag", format="%d"),
            "pu_name": st.column_config.TextColumn("Pickup-Zone"),
            "tip_pct": st.column_config.NumberColumn("Tip (%)", format="%.2f %%"),
        },
    )
