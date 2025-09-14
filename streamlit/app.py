import streamlit as st, pandas as pd
from sqlalchemy import create_engine

st.set_page_config(page_title="Taxi Pipe – Smoke Test", layout="wide")
st.title("End-to-End: Kafka → Spark → Postgres → Streamlit")

engine = create_engine("postgresql+psycopg2://nyc:nyc@localhost:5432/nyc")

total = pd.read_sql("SELECT COUNT(*) AS rows FROM rides", engine)
st.metric("Datensätze in Postgres", f"{int(total.iloc[0]['rows']):,}")

svc = pd.read_sql("""
  SELECT service_type, COUNT(*) AS rows,
         AVG(fare_amount)::numeric(10,2) AS avg_fare,
         AVG(trip_distance)::numeric(10,2) AS avg_dist
  FROM rides
  GROUP BY service_type
  ORDER BY service_type
""", engine)
st.subheader("Yellow vs. Green")
st.dataframe(svc)

ts = pd.read_sql("""
  SELECT date_trunc('hour', pickup_datetime) AS hour,
         service_type, COUNT(*) AS rides
  FROM rides
  GROUP BY 1,2 ORDER BY 1
""", engine)
if not ts.empty:
    st.subheader("Fahrten pro Stunde")
    st.line_chart(ts.pivot(index="hour", columns="service_type", values="rides"))
