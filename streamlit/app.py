import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Taxi Pipe – Dashboard", layout="wide")
st.title("End-to-End: Kafka → Spark → Postgres → Streamlit")

# DB via ENV konfigurierbar (für Docker später praktisch)
DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://nyc:nyc@localhost:5432/nyc")

@st.cache_resource
def get_engine():
    return create_engine(DB_URL)

engine = get_engine()

# ----- Sidebar: Filter -----
with st.sidebar:
    st.header("Filter")
    # Datumsgrenzen aus DB
    bounds = pd.read_sql("SELECT min(pickup_datetime) AS dmin, max(pickup_datetime) AS dmax FROM rides", engine)
    dmin = (bounds["dmin"].iloc[0] or pd.Timestamp("today")).date()
    dmax = (bounds["dmax"].iloc[0] or pd.Timestamp("today")).date()
    start, end = st.date_input("Zeitraum", (dmin, dmax))
    services = st.multiselect("Service", ["yellow", "green"], default=["yellow", "green"])
    smooth = st.checkbox("Glätten (Rolling 3)", value=False)
    if st.button("Neu laden"):
        st.experimental_rerun()
    if not services:
        st.stop()

params = {"start": pd.Timestamp(start), "end": pd.Timestamp(end) + pd.Timedelta(days=1), "svc": services}

# ----- KPIs -----
kpi_sql = text("""
    SELECT
      COUNT(*)                            AS rows,
      AVG(fare_amount)::numeric(10,2)     AS avg_fare,
      AVG(trip_distance)::numeric(10,2)   AS avg_dist,
      SUM(total_amount)::numeric(12,2)    AS revenue
    FROM rides
    WHERE pickup_datetime >= :start AND pickup_datetime < :end
      AND service_type = ANY(:svc)
""")
kpi = pd.read_sql(kpi_sql, engine, params=params).iloc[0]
c1,c2,c3,c4 = st.columns(4)
c1.metric("Datensätze in Postgres", f"{int(kpi['rows']):,}")
c2.metric("Ø Fare ($)",  kpi["avg_fare"])
c3.metric("Ø Distanz (mi)", kpi["avg_dist"])
c4.metric("Umsatz ($)", kpi["revenue"])

# ----- Yellow vs. Green -----
svc_sql = text("""
  SELECT service_type,
         COUNT(*) AS rows,
         AVG(fare_amount)::numeric(10,2) AS avg_fare,
         AVG(trip_distance)::numeric(10,2) AS avg_dist
  FROM rides
  WHERE pickup_datetime >= :start AND pickup_datetime < :end
    AND service_type = ANY(:svc)
  GROUP BY service_type
  ORDER BY service_type
""")
svc_df = pd.read_sql(svc_sql, engine, params=params)
st.subheader("Yellow vs. Green")
st.dataframe(svc_df, use_container_width=True)

# ----- Zeitreihe pro Stunde -----
ts_sql = text("""
  SELECT date_trunc('hour', pickup_datetime) AS hr,
         service_type, COUNT(*) AS rows
  FROM rides
  WHERE pickup_datetime >= :start AND pickup_datetime < :end
    AND service_type = ANY(:svc)
  GROUP BY hr, service_type
  ORDER BY hr
""")
ts = pd.read_sql(ts_sql, engine, params=params)
st.subheader("Fahrten pro Stunde")
if ts.empty:
    st.info("Keine Daten im gewählten Zeitraum/Filter.")
else:
    ts = ts.sort_values("hr")
    if smooth:
        ts["rows"] = ts.groupby("service_type")["rows"].transform(lambda s: s.rolling(3, min_periods=1).mean())
    pivot = ts.pivot(index="hr", columns="service_type", values="rows")
    st.line_chart(pivot, use_container_width=True)
