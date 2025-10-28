# app.py — Event-driven Live-Uhr (jede NOTIFY = ein Schritt, kein Endlos-Rerun)
# Zeigt die pickup_datetime GENAU des Datensatzes aus der NOTIFY-Payload (table + id).
# Fallback bei Leerlauf: neuester per ingested_at. Nur bei Events wird neu gerendert.

import os
import json
import select
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
import psycopg2

# ================= Helpers =================

def _safe_rerun():
    try:
        st.rerun()
    except AttributeError:
        st.experimental_rerun()

def _ensure_utc(ts):
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return None
    ts = pd.to_datetime(ts)
    return ts.tz_localize(timezone.utc) if ts.tzinfo is None else ts.tz_convert(timezone.utc)

def _fmt(ts, tz="UTC"):
    if ts is None:
        return "—"
    return _ensure_utc(ts).tz_convert(ZoneInfo(tz)).strftime("%Y-%m-%d %H:%M:%S %Z")

def _normalize_table_name(raw: str) -> str:
    """
    Akzeptiert: 'rides_yellow', 'public.rides_yellow', 'yellow' (dito für green)
    Liefert:   'rides_yellow' oder 'rides_green'
    """
    if not raw:
        return ""
    t = raw.lower().strip()
    if "." in t:
        t = t.split(".")[-1]
    if t in ("yellow", "rides_yellow"):
        return "rides_yellow"
    if t in ("green", "rides_green"):
        return "rides_green"
    return ""

# ================= Settings =================

st.set_page_config(page_title="Taxi Livezeit (Event-Driven)", layout="wide")

DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://nyc:nyc@postgres:5432/nyc")
DB_URL_PG = DB_URL.replace("postgresql+psycopg2://", "postgresql://")
UI_TZ = os.getenv("UI_TZ", "Europe/Vienna")
LISTEN_CHANNEL = os.getenv("LISTEN_CHANNEL", "rides_new")
WAIT_TIMEOUT_SEC = int(os.getenv("WAIT_TIMEOUT_SEC", "60"))

# ================= DB Ressourcen =================

@st.cache_resource
def get_engine():
    return create_engine(DB_URL, pool_size=2, max_overflow=0)

def _pg_connect():
    return psycopg2.connect(
        DB_URL_PG,
        keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5,
        application_name="streamlit_live_clock",
    )

@st.cache_resource
def get_listen_conn():
    conn = _pg_connect()
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    with conn.cursor() as cur:
        cur.execute(f"LISTEN {LISTEN_CHANNEL};")
    return conn

engine = get_engine()
listen_conn = get_listen_conn()

def _relisten():
    """Neu verbinden & LISTEN neu setzen (ohne sofortigen Rerun)."""
    global listen_conn
    try:
        listen_conn.close()
    except Exception:
        pass
    listen_conn = _pg_connect()
    listen_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    with listen_conn.cursor() as cur:
        cur.execute(f"LISTEN {LISTEN_CHANNEL};")

# ================= Queries =================

SQL_BY_ID = {
    "rides_yellow": text("""
        SELECT 'yellow'::text AS service_type,
               id, pickup_datetime, dropoff_datetime,
               pu_loc, do_loc, passenger_count, payment_type, ingested_at
        FROM public.rides_yellow WHERE id = :id
    """),
    "rides_green": text("""
        SELECT 'green'::text AS service_type,
               id, pickup_datetime, dropoff_datetime,
               pu_loc, do_loc, passenger_count, payment_type, ingested_at
        FROM public.rides_green WHERE id = :id
    """),
}

LATEST_SQL = text("""
WITH latest_y AS (
  SELECT 'yellow'::text AS service_type,
         id, pickup_datetime, dropoff_datetime,
         pu_loc, do_loc, passenger_count, payment_type, ingested_at
  FROM public.rides_yellow
  ORDER BY ingested_at DESC, id DESC
  LIMIT 1
),
latest_g AS (
  SELECT 'green'::text AS service_type,
         id, pickup_datetime, dropoff_datetime,
         pu_loc, do_loc, passenger_count, payment_type, ingested_at
  FROM public.rides_green
  ORDER BY ingested_at DESC, id DESC
  LIMIT 1
)
SELECT * FROM (SELECT * FROM latest_y UNION ALL SELECT * FROM latest_g) x
ORDER BY ingested_at DESC, id DESC
LIMIT 1;
""")

def fetch_by_id(table: str, row_id: int) -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(SQL_BY_ID[table], conn, params={"id": int(row_id)})

def fetch_latest_row() -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(LATEST_SQL, conn)

# ================= Event-State =================

if "event_queue" not in st.session_state:
    st.session_state.event_queue = []  # [{table:'rides_green', id:12345}, ...]
if "last_event_utc" not in st.session_state:
    st.session_state.last_event_utc = None
if "debug_last_payloads" not in st.session_state:
    st.session_state.debug_last_payloads = []

def _append_notifications_to_queue():
    """Alle pending NOTIFYs lesen und in die Session-Queue legen."""
    try:
        listen_conn.poll()
    except Exception:
        _relisten()
        return
    while listen_conn.notifies:
        note = listen_conn.notifies.pop(0)
        payload_txt = note.payload or ""
        try:
            payload = json.loads(payload_txt)
        except Exception:
            payload = {}
        st.session_state.debug_last_payloads = (
            (st.session_state.debug_last_payloads + [payload_txt])[-5:]
        )
        tbl = _normalize_table_name(payload.get("table", ""))
        rid = payload.get("id")
        if tbl and rid is not None:
            st.session_state.event_queue.append({"table": tbl, "id": int(rid)})
            st.session_state.last_event_utc = datetime.now(timezone.utc)

# ================= UI =================

st.markdown("<h1 style='margin-bottom:0'>Live-Zeit (neuester DB-Eintrag)</h1>", unsafe_allow_html=True)

# 1) Wenn Events in der Queue → erstes Event ziehen
row_df = None
if st.session_state.event_queue:
    ev = st.session_state.event_queue.pop(0)
    row_df = fetch_by_id(ev["table"], ev["id"])
else:
    # 2) Prüfen, ob bereits Notifications im Socket liegen → ggf. in Queue und sofort nutzen
    _append_notifications_to_queue()
    if st.session_state.event_queue:
        ev = st.session_state.event_queue.pop(0)
        row_df = fetch_by_id(ev["table"], ev["id"])

# 3) Fallback (kein Event): neuester Datensatz per ingested_at
if row_df is None or row_df.empty:
    row_df = fetch_latest_row()

if row_df.empty:
    st.info("Noch keine Daten in rides_yellow / rides_green.")
else:
    r = row_df.iloc[0]
    srv  = r["service_type"]
    pick = _ensure_utc(r["pickup_datetime"])
    drop = _ensure_utc(r["dropoff_datetime"])
    ing  = _ensure_utc(r["ingested_at"])

    st.markdown(
        f"""
        <div style="margin-top:6px; line-height:1.15;">
          <div style="font-size:36px; font-weight:800;">{_fmt(pick, UI_TZ)}</div>
          <div style="font-size:12px; color:#888;">{_fmt(pick, "UTC")}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    c1, c2 = st.columns(2)
    with c1:
        st.write("**Service**")
        st.write(srv.upper())
    with c2:
        st.write("**Row-ID**")
        st.write(str(int(r["id"])) if pd.notna(r["id"]) else "—")

    if st.session_state.last_event_utc:
        ago = datetime.now(timezone.utc) - st.session_state.last_event_utc
        st.caption(f"Letztes NOTIFY vor {int(ago.total_seconds())}s • Kanal „{LISTEN_CHANNEL}“")

    with st.expander("Details des neuesten Eintrags"):
        st.write({
            "service_type": srv,
            "id": int(r["id"]) if pd.notna(r["id"]) else None,
            "pickup_datetime_vienna": _fmt(pick, UI_TZ),
            "pickup_datetime_utc": _fmt(pick, "UTC"),
            "dropoff_datetime_vienna": _fmt(drop, UI_TZ),
            "pu_loc": r["pu_loc"],
            "do_loc": r["do_loc"],
            "passenger_count": r["passenger_count"],
            "payment_type": r["payment_type"],
            "ingested_at_vienna": _fmt(ing, UI_TZ),
            "ingested_at_utc": _fmt(ing, "UTC"),
        })

st.markdown("<hr style='opacity:0.2'/>", unsafe_allow_html=True)
st.caption(f"Event-Driven via LISTEN/NOTIFY auf „{LISTEN_CHANNEL}“ • Timeout {WAIT_TIMEOUT_SEC}s")

# ================= Event-Wait (nur bei Events rerun) =================

# a) Schon anliegende NOTIFYs → in Queue, ggf. sofort rerun
_append_notifications_to_queue()
if st.session_state.event_queue:
    st.cache_data.clear()
    _safe_rerun()

# b) Blockierend warten – NUR bei echtem Event rerun, sonst Seite stehen lassen
try:
    readable, _, _ = select.select([listen_conn], [], [], WAIT_TIMEOUT_SEC)
except Exception:
    _relisten()
    readable = []

if readable:
    _append_notifications_to_queue()
    if st.session_state.event_queue:
        st.cache_data.clear()
        _safe_rerun()
# Kein Event (Timeout) -> KEIN rerun. Seite bleibt ruhig stehen.
