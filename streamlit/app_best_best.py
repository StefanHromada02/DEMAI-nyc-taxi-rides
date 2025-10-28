# app.py — Event-driven Live-Uhr (jede NOTIFY = ein Schritt, robustes Service-Mapping)
# Zeigt die pickup_datetime GENAU des Datensatzes aus der NOTIFY (table/service + id).
# Fallback bei Leerlauf: neuester per ingested_at. Re-Run nur bei echten Events.

import os
import json
import re
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
    Akzeptiert z.B.:
      'rides_yellow', 'public.rides_yellow', 'yellow'
      'rides_green',  'public.rides_green',  'green'
    Liefert: 'rides_yellow' / 'rides_green' oder '' wenn unklar
    """
    if not raw:
        return ""
    t = str(raw).lower().strip()
    if "." in t:
        t = t.split(".")[-1]
    if t in ("yellow", "rides_yellow"):
        return "rides_yellow"
    if t in ("green", "rides_green"):
        return "rides_green"
    return ""

def _parse_payload(txt: str) -> tuple[str, int | None, str]:
    """
    Versucht, aus der Payload (JSON ODER Freitext) Tabelle/Service und ID zu extrahieren.
    Rückgabe: (parsed_table_norm | '', parsed_id | None, debug_reason)
    """
    if not txt:
        return "", None, "empty"
    # 1) JSON versuchen
    try:
        obj = json.loads(txt)
        # Tolerant bei Keys: table/tbl/rel, service_type/service
        cand_tbl = obj.get("table") or obj.get("tbl") or obj.get("rel") or obj.get("relation")
        cand_srv = obj.get("service_type") or obj.get("service")
        cand_id  = obj.get("id") or obj.get("pk") or obj.get("row_id")
        tbl_norm = _normalize_table_name(cand_tbl or cand_srv or "")
        if tbl_norm or cand_id is not None:
            return tbl_norm, (int(cand_id) if cand_id is not None else None), "json"
    except Exception:
        pass
    # 2) Freitext: nach green/yellow und Zahlen-ID suchen
    low = txt.lower()
    tbl_norm = ""
    if "green" in low or "rides_green" in low:
        tbl_norm = "rides_green"
    elif "yellow" in low or "rides_yellow" in low:
        tbl_norm = "rides_yellow"
    m = re.search(r'"?id"?\s*[:=]\s*(\d+)', low) or re.search(r'\b(\d{1,12})\b', low)
    cand_id = int(m.group(1)) if m else None
    return tbl_norm, cand_id, "heuristic"

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

def _query_one(table_key: str, row_id: int) -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(SQL_BY_ID[table_key], conn, params={"id": int(row_id)})

def _query_both_and_choose(row_id: int) -> tuple[pd.DataFrame, str]:
    """
    Holt dieselbe id aus beiden Tabellen und nimmt die mit der neueren ingested_at.
    (Falls nur eine existiert, wird die genommen.)
    """
    df_y = _query_one("rides_yellow", row_id)
    df_g = _query_one("rides_green", row_id)
    if df_y.empty and df_g.empty:
        return pd.DataFrame(), ""
    if df_y.empty:
        return df_g, "rides_green"
    if df_g.empty:
        return df_y, "rides_yellow"
    # beide da -> neuere ingested_at gewinnt
    iy = pd.to_datetime(df_y.iloc[0]["ingested_at"])
    ig = pd.to_datetime(df_g.iloc[0]["ingested_at"])
    return (df_y, "rides_yellow") if iy >= ig else (df_g, "rides_green")

def fetch_by_event(payload_text: str) -> tuple[pd.DataFrame, str, str, str, int | None]:
    """
    Lädt den Datensatz passend zum Event.
    Rückgabe: (df, payload_table_norm, parsed_from, resolved_table_used, parsed_id)
    """
    tbl_norm, rid, parsed_from = _parse_payload(payload_text)
    if rid is None:
        return pd.DataFrame(), tbl_norm, parsed_from, "", None
    if tbl_norm in SQL_BY_ID:
        df = _query_one(tbl_norm, rid)
        if not df.empty:
            return df, tbl_norm, parsed_from, tbl_norm, rid
        # Fallback: andere Tabelle probieren
        other = "rides_green" if tbl_norm == "rides_yellow" else "rides_yellow"
        df2 = _query_one(other, rid)
        if not df2.empty:
            return df2, tbl_norm, parsed_from, other, rid
        return pd.DataFrame(), tbl_norm, parsed_from, "", rid
    # Unklar → beide probieren und anhand ingested_at entscheiden
    df, resolved = _query_both_and_choose(rid)
    return df, tbl_norm, parsed_from, resolved, rid

def fetch_latest_row() -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(LATEST_SQL, conn)

# ================= Event-State =================

if "event_queue" not in st.session_state:
    st.session_state.event_queue = []  # [{'payload': '<raw text>'}, ...]
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
        st.session_state.debug_last_payloads = (
            (st.session_state.debug_last_payloads + [payload_txt])[-10:]
        )
        st.session_state.event_queue.append({"payload": payload_txt})
        st.session_state.last_event_utc = datetime.now(timezone.utc)

# ================= UI =================

st.markdown("<h1 style='margin-bottom:0'>Live-Zeit (neuester DB-Eintrag)</h1>", unsafe_allow_html=True)

# 1) Event aus Queue ziehen (falls vorhanden)
row_df = None
payload_tbl_norm = ""
parsed_from = ""
resolved_tbl = ""
parsed_id = None

if st.session_state.event_queue:
    ev = st.session_state.event_queue.pop(0)
    row_df, payload_tbl_norm, parsed_from, resolved_tbl, parsed_id = fetch_by_event(ev["payload"])
else:
    _append_notifications_to_queue()
    if st.session_state.event_queue:
        ev = st.session_state.event_queue.pop(0)
        row_df, payload_tbl_norm, parsed_from, resolved_tbl, parsed_id = fetch_by_event(ev["payload"])

# 2) Fallback (kein Event oder nichts gefunden): neuester Datensatz per ingested_at
fallback_used = False
if row_df is None or row_df.empty:
    row_df = fetch_latest_row()
    fallback_used = True

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
        status = f"Letztes NOTIFY vor {int(ago.total_seconds())}s • Kanal „{LISTEN_CHANNEL}“"
        if not fallback_used:
            status += (
                f" • payload_table={payload_tbl_norm or '—'}"
                f" • parsed={parsed_from or '—'}"
                f" • resolved={resolved_tbl or '—'}"
                f" • id={parsed_id if parsed_id is not None else '—'}"
            )
        st.caption(status)

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

with st.expander("Debug: letzte Payloads"):
    st.code("\n".join(st.session_state.debug_last_payloads) or "—", language="json")

st.markdown("<hr style='opacity:0.2'/>", unsafe_allow_html=True)
st.caption(f"Event-Driven via LISTEN/NOTIFY auf „{LISTEN_CHANNEL}“ • Timeout {WAIT_TIMEOUT_SEC}s")

# ================= Event-Wait (nur bei Events rerun) =================

_append_notifications_to_queue()
if st.session_state.event_queue:
    st.cache_data.clear()
    _safe_rerun()

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
# Kein Event -> kein Rerun.
