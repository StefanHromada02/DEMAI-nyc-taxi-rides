import time
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from collections import deque

# -----------------------------------------------------------------------------
# Seite & DB
# -----------------------------------------------------------------------------
st.set_page_config(page_title="NYC Taxi Stream (live)", layout="wide")
DB_URL = "postgresql+psycopg2://nyc:nyc@localhost:5432/nyc"
engine = create_engine(DB_URL, pool_pre_ping=True)

st.title("NYC Taxi – Live View")

# -----------------------------------------------------------------------------
# Sidebar: Moduswahl
# -----------------------------------------------------------------------------
mode = st.sidebar.radio("Modus", ["Snapshot (intervall)", "Live (kontinuierlich)"])
n_show = st.sidebar.slider("Anzahl Zeilen anzeigen", 5, 200, 50, step=5)
services = st.sidebar.multiselect("Service-Filter", ["yellow", "green"], default=[])

# -----------------------------------------------------------------------------
# Gemeinsame Hilfen
# -----------------------------------------------------------------------------
def base_query(limit=None, min_id=None):
    where = []
    if services:
        where.append("service_type = ANY(:services)")
    if min_id is not None:
        where.append("id > :min_id")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    limit_sql = f"LIMIT {int(limit)}" if limit else ""
    sql = f"""
        SELECT id, service_type, pickup_datetime, dropoff_datetime,
               trip_distance, fare_amount, tip_amount, total_amount,
               pu_loc, do_loc
        FROM rides
        {where_sql}
        ORDER BY pickup_datetime DESC
        {limit_sql}
    """
    return sql

def fetch_latest(limit=50, min_id=None):
    params = {}
    if services:
        params["services"] = services
    if min_id is not None:
        params["min_id"] = int(min_id)
    with engine.connect() as con:
        df = pd.read_sql(text(base_query(limit=limit, min_id=min_id)), con, params=params)
    return df

# -----------------------------------------------------------------------------
# SNAPSHOT-MODUS (Intervall mit Auto-Refresh)
# -----------------------------------------------------------------------------
if mode == "Snapshot (intervall)":
    refresh_sec = st.sidebar.slider("Auto-Refresh (Sek.)", 0, 30, 5, help="0 = aus")
    manual = st.sidebar.button("Jetzt aktualisieren")

    # Auto-Refresh: sanft, ohne kompletten manuellen Loop
    try:
        from streamlit import st_autorefresh  # type: ignore[attr-defined]
    except Exception:
        st_autorefresh = getattr(st, "st_autorefresh", None)

    if callable(st_autorefresh) and refresh_sec > 0:
        st_autorefresh(interval=refresh_sec * 1000, key="auto-refresh-snap")

    if manual:
        st.rerun()

    @st.cache_data(ttl=max(1, refresh_sec) if refresh_sec > 0 else 15, show_spinner=False)
    def load_snapshot(n: int):
        return fetch_latest(limit=n)

    df = load_snapshot(n_show)

    left, right = st.columns([2, 1])
    with left:
        st.subheader("Neueste Fahrten")
        st.dataframe(df, use_container_width=True, height=420)

    with right:
        st.subheader("KPIs")
        st.metric("Anzahl Zeilen", int(len(df)))
        if not df.empty:
            st.metric("Ø Fare", f"{df['fare_amount'].mean():.2f}")
            st.metric("Ø Distanz (mi)", f"{df['trip_distance'].mean():.2f}")
        st.subheader("Service Split")
        if not df.empty:
            st.bar_chart(df.groupby("service_type")["id"].count())
        else:
            st.info("Keine Daten im aktuellen Filter.")
    st.caption("Datenquelle: PostgreSQL Tabelle 'rides'")

# -----------------------------------------------------------------------------
# LIVE-MODUS (kontinuierliches Polling + inkrementelles Append)
# -----------------------------------------------------------------------------
else:
    poll_sec = st.sidebar.slider("Polling-Intervall (Sek.)", 1, 10, 2)
    max_buffer = st.sidebar.slider("Max. Pufferzeilen live", 50, 5000, 500, step=50)
    col1, col2 = st.columns(2)
    with col1:
        go_live = st.button("▶ Live starten", type="primary")
    with col2:
        stop_live = st.button("⏹ Live stoppen")

    # Session-Buffer halten (nur für diese Browser-Session)
    if "live_buffer" not in st.session_state:
        st.session_state.live_buffer = deque(maxlen=max_buffer)
    if "last_id" not in st.session_state:
        st.session_state.last_id = None
    if "live_running" not in st.session_state:
        st.session_state.live_running = False

    # Start/Stop toggeln
    if go_live:
        st.session_state.live_running = True
    if stop_live:
        st.session_state.live_running = False

    # Platzhalter fürs UI (wird im Loop aktualisiert)
    table_ph = st.empty()
    kpi_ph = st.empty()
    chart_ph = st.empty()

    # Initiale Befüllung (letzte n_show Zeilen als Start)
    if not st.session_state.live_buffer:
        seed = fetch_latest(limit=n_show).sort_values("id")  # aufsteigend
        for _, row in seed.iterrows():
            st.session_state.live_buffer.append(row.to_dict())
        if not seed.empty:
            st.session_state.last_id = int(seed["id"].max())

    def render_now():
        # DataFrame aus Buffer bauen, nur die letzten n_show Zeilen zeigen
        if st.session_state.live_buffer:
            df_now = pd.DataFrame(list(st.session_state.live_buffer))[-n_show:]
        else:
            df_now = pd.DataFrame(columns=[
                "id","service_type","pickup_datetime","dropoff_datetime",
                "trip_distance","fare_amount","tip_amount","total_amount","pu_loc","do_loc"
            ])

        left, right = st.columns([2, 1])
        with left:
            st.subheader("Neueste Fahrten (live)")
            table_ph.dataframe(df_now.sort_values("pickup_datetime", ascending=False),
                               use_container_width=True, height=420)

        with right:
            with kpi_ph.container():
                st.subheader("KPIs")
                st.metric("Anzahl Zeilen (im Blick)", int(len(df_now)))
                if not df_now.empty:
                    st.metric("Ø Fare", f"{df_now['fare_amount'].mean():.2f}")
                    st.metric("Ø Distanz (mi)", f"{df_now['trip_distance'].mean():.2f}")
                st.subheader("Service Split")
                if not df_now.empty:
                    chart_ph.bar_chart(df_now.groupby("service_type")["id"].count())
                else:
                    st.info("Warte auf Daten …")

    # Einmal rendern
    render_now()

    # Live-Loop: pollt inkrementell per last_id
    if st.session_state.live_running:
        info = st.info("🔴 Live läuft … (Polling)")
        try:
            while st.session_state.live_running:
                # neue Zeilen holen (id > last_id)
                new_df = fetch_latest(limit=None, min_id=st.session_state.last_id).sort_values("id")
                if not new_df.empty:
                    for _, row in new_df.iterrows():
                        st.session_state.live_buffer.append(row.to_dict())
                    st.session_state.last_id = int(new_df["id"].max())
                    render_now()  # UI aktualisieren nur wenn es Neues gibt
                time.sleep(poll_sec)
        finally:
            info.empty()

    st.caption("Datenquelle: PostgreSQL Tabelle 'rides'")
