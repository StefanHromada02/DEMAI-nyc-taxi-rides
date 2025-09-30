# analysis_report.py
# Läuft ohne Streamlit: zieht alle Metriken/Tabellen aus Postgres und speichert Plots.
import os
import argparse
from typing import List

import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text


# -------------------- CLI & Config --------------------
def parse_args():
    p = argparse.ArgumentParser(description="NYC Taxi – SQL Report & Plots")
    p.add_argument("--db-url",
                   default=os.getenv("DB_URL", "postgresql+psycopg2://nyc:nyc@localhost:5432/nyc"),
                   help="SQLAlchemy DB URL (Default: env DB_URL oder local)")
    p.add_argument("--start", default=None, help="Startdatum (YYYY-MM-DD). Default: min(pickup_datetime)")
    p.add_argument("--end",   default=None, help="Enddatum (YYYY-MM-DD). Default: max(pickup_datetime)")
    p.add_argument("--services", default="yellow,green", help="Komma-getrennt: z.B. 'yellow' oder 'yellow,green'")
    p.add_argument("--smooth", action="store_true", help="Glätten (Rolling 3) für Zeitreihe (Stunden)")
    p.add_argument("--outdir", default="figures", help="Ordner zum Speichern der Plots")
    return p.parse_args()


# -------------------- DB Helper --------------------
def get_engine(db_url: str):
    return create_engine(db_url, pool_pre_ping=True)

def fetch_df(engine, sql: str, params=None) -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params)

def get_bounds(engine):
    b = fetch_df(engine, "SELECT min(pickup_datetime) AS dmin, max(pickup_datetime) AS dmax FROM rides")
    return pd.to_datetime(b.dmin.iloc[0]), pd.to_datetime(b.dmax.iloc[0])


# -------------------- Queries --------------------
KPI_SQL = """
SELECT COUNT(*) AS rows,
       AVG(fare_amount)::numeric(10,2)   AS avg_fare,
       AVG(trip_distance)::numeric(10,2) AS avg_dist,
       SUM(total_amount)::numeric(12,2)  AS revenue
FROM rides
WHERE pickup_datetime >= :start AND pickup_datetime < :end
  AND service_type = ANY(:svc)
"""

SVC_SQL = """
SELECT service_type,
       COUNT(*) AS rows,
       AVG(fare_amount)::numeric(10,2)   AS avg_fare,
       AVG(trip_distance)::numeric(10,2) AS avg_dist
FROM rides
WHERE pickup_datetime >= :start AND pickup_datetime < :end
  AND service_type = ANY(:svc)
GROUP BY service_type
ORDER BY service_type
"""

TS_SQL = """
SELECT date_trunc('hour', pickup_datetime) AS hr,
       service_type, COUNT(*) AS rows
FROM rides
WHERE pickup_datetime >= :start AND pickup_datetime < :end
  AND service_type = ANY(:svc)
GROUP BY hr, service_type
ORDER BY hr
"""

HOT_SQL = """
WITH base AS (
  SELECT
    r.pickup_datetime,
    COALESCE(zpu."Zone", 'ID '||r.pu_loc::text) AS pu_name,
    COALESCE(zdo."Zone", 'ID '||r.do_loc::text) AS do_name
  FROM rides r
  LEFT JOIN taxi_zones zpu ON zpu."LocationID" = r.pu_loc
  LEFT JOIN taxi_zones zdo ON zdo."LocationID" = r.do_loc
  WHERE r.pickup_datetime >= :start AND r.pickup_datetime < :end
    AND r.service_type = ANY(:svc)
)
SELECT
  date_trunc(:grain, pickup_datetime) AS period,
  COUNT(*) FILTER (WHERE pu_name IS NOT NULL) AS pu_count,
  COUNT(*) FILTER (WHERE do_name IS NOT NULL) AS do_count,
  pu_name, do_name
FROM base
GROUP BY period, pu_name, do_name
ORDER BY period
"""

HAIL_SQL = """
SELECT date_trunc(:grain, pickup_datetime) AS period,
       trip_type,
       COUNT(*) AS rows
FROM rides
WHERE pickup_datetime >= :start AND pickup_datetime < :end
  AND service_type = ANY(:svc)
  AND trip_type IS NOT NULL
GROUP BY period, trip_type
ORDER BY period
"""

VENDOR_SQL = """
SELECT date_trunc(:grain, pickup_datetime) AS period,
       vendor_id, COUNT(*) AS rows
FROM rides
WHERE pickup_datetime >= :start AND pickup_datetime < :end
  AND service_type = ANY(:svc)
  AND vendor_id IS NOT NULL
GROUP BY period, vendor_id
ORDER BY period, rows DESC
"""

RUSH_SQL = """
SELECT
  EXTRACT(ISODOW FROM r.pickup_datetime)::int AS weekday,
  EXTRACT(HOUR   FROM r.pickup_datetime)::int AS hour,
  COALESCE(zpu."Zone", 'ID '||r.pu_loc::text) AS pu_name,
  COUNT(*) AS rides
FROM rides r
LEFT JOIN taxi_zones zpu ON zpu."LocationID" = r.pu_loc
WHERE r.pickup_datetime >= :start AND r.pickup_datetime < :end
  AND r.service_type = ANY(:svc)
GROUP BY weekday, hour, pu_name
ORDER BY weekday, hour, rides DESC
"""

TIP_SQL = """
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
FROM rides r
LEFT JOIN taxi_zones zpu ON zpu."LocationID" = r.pu_loc
WHERE r.pickup_datetime >= :start AND r.pickup_datetime < :end
  AND r.service_type = ANY(:svc)
GROUP BY weekday, pu_name
ORDER BY weekday, tip_pct DESC
"""


# -------------------- Plots --------------------
def ensure_outdir(path: str):
    os.makedirs(path, exist_ok=True)

def plot_time_series(df: pd.DataFrame, outdir: str, smooth: bool, title_suffix: str = ""):
    if df.empty:
        return
    df = df.sort_values("hr")
    if smooth:
        df["rows"] = df.groupby("service_type")["rows"].transform(lambda s: s.rolling(3, min_periods=1).mean())
    pivot = df.pivot(index="hr", columns="service_type", values="rows")
    ax = pivot.plot(figsize=(10, 4), marker="o")
    ax.set_title(f"Fahrten pro Stunde{(' – ' + title_suffix) if title_suffix else ''}")
    ax.set_xlabel("Stunde")
    ax.set_ylabel("Fahrten")
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, "timeseries_hourly.png"))
    plt.close()

def plot_hotspots(df: pd.DataFrame, outdir: str, grain_label: str):
    if df.empty:
        return
    last_period = df["period"].max()
    slice_df = df[df["period"] == last_period].copy()

    pu = (slice_df.groupby("pu_name", as_index=False)["pu_count"].sum()
          .sort_values("pu_count", ascending=False).head(10))
    do = (slice_df.groupby("do_name", as_index=False)["do_count"].sum()
          .sort_values("do_count", ascending=False).head(10))

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.barh(pu["pu_name"][::-1], pu["pu_count"][::-1])
    ax.set_title(f"Top 10 Pickup-Zonen – {grain_label} {last_period.date()}")
    ax.set_xlabel("Fahrten")
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, f"hotspots_pickup_{grain_label}.png"))
    plt.close()

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.barh(do["do_name"][::-1], do["do_count"][::-1])
    ax.set_title(f"Top 10 Dropoff-Zonen – {grain_label} {last_period.date()}")
    ax.set_xlabel("Fahrten")
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, f"hotspots_dropoff_{grain_label}.png"))
    plt.close()

def plot_vendor(df: pd.DataFrame, outdir: str, grain_label: str):
    if df.empty:
        return
    last_period = df["period"].max()
    slic = df[df["period"] == last_period].nlargest(5, "rows")
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(slic["vendor_id"].astype(str), slic["rows"])
    ax.set_title(f"Top VendorIDs – {grain_label} {last_period.date()}")
    ax.set_xlabel("VendorID")
    ax.set_ylabel("Fahrten")
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, f"vendor_top_{grain_label}.png"))
    plt.close()

def plot_rush_heatmap(df: pd.DataFrame, outdir: str, title_suffix: str = ""):
    if df.empty:
        return
    grid = (df.groupby(["weekday", "hour"])["rides"]
              .sum().unstack(fill_value=0)
              .reindex(index=range(1, 8), columns=range(0, 24), fill_value=0))
    fig, ax = plt.subplots(figsize=(10, 3))
    im = ax.imshow(grid.values, aspect="auto")
    ax.set_title(f"Rush Heatmap – Fahrten (Wochentag × Stunde){(' – ' + title_suffix) if title_suffix else ''}")
    ax.set_yticks(range(0, 7), labels=[1, 2, 3, 4, 5, 6, 7])
    ax.set_xticks(range(0, 24, 2))
    ax.set_xlabel("Stunde")
    ax.set_ylabel("Wochentag (ISO)")
    fig.colorbar(im, ax=ax, label="Fahrten")
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, "rush_heatmap.png"))
    plt.close()

def plot_tip(df: pd.DataFrame, outdir: str, title_suffix: str = ""):
    if df.empty:
        return
    top = (df.groupby("pu_name", as_index=False)["tip_pct"].mean()
           .sort_values("tip_pct", ascending=False).head(10))
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.barh(top["pu_name"][::-1], top["tip_pct"][::-1])
    ax.set_title(f"Top 10 Pickup-Zonen nach Tip-% (Ø über Wochentage){(' – ' + title_suffix) if title_suffix else ''}")
    ax.set_xlabel("Tip (%)")
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, "tip_top10.png"))
    plt.close()


# -------------------- Main --------------------
def main():
    args = parse_args()
    services: List[str] = [s.strip() for s in args.services.split(",") if s.strip()]
    engine = get_engine(args.db_url)
    ensure_outdir(args.outdir)

    # Globales Fenster bestimmen (für "jährlich")
    dmin, dmax = get_bounds(engine)
    start = pd.to_datetime(args.start) if args.start else dmin.normalize()
    end   = pd.to_datetime(args.end)   if args.end   else (dmax.normalize() + pd.Timedelta(days=1))

    # Abgeleitete Fenster:
    # - Monatlich: letzter Monat im globalen Fenster [month_start, end)
    # - Stündlich: letzter Tag im globalen Fenster [day_start, day_start+1)
    last_instant = end - pd.Timedelta(seconds=1)     # letzter Zeitpunkt im Fenster
    month_start  = last_instant.to_period("M").start_time
    day_start    = last_instant.normalize()
    day_end      = day_start + pd.Timedelta(days=1)

    params_year  = {"start": start,      "end": end,   "svc": services}     # "jährlich" = global
    params_month = {"start": month_start,"end": end,   "svc": services}     # nur letzter Monatsteil
    params_hour  = {"start": day_start,  "end": day_end,"svc": services}    # nur letzter Tag

    # Logging
    print(f"\n[Global/Jahr]   {start} → {end} | Services: {services}")
    print(f"[Monat (last)]  {month_start} → {end}")
    print(f"[Stunde (last)] {day_start} → {day_end}\n")

    # -------- KPIs + Service-Vergleich (globales Fenster) --------
    kpi = fetch_df(engine, KPI_SQL, params_year).iloc[0]
    print("=== KPIs (global) ===")
    print(f"Zeilen: {int(kpi['rows']):,}")
    print(f"Ø Fare ($): {kpi['avg_fare']:.2f}")
    print(f"Ø Distanz (mi): {kpi['avg_dist']:.2f}")
    print(f"Umsatz ($): {kpi['revenue']:.2f}\n")

    svc_df = fetch_df(engine, SVC_SQL, params_year)
    print("=== Yellow vs. Green (global) ===")
    print(svc_df.to_string(index=False), "\n")

    # -------- Zeitreihe (nur letzter Tag) --------
    ts = fetch_df(engine, TS_SQL, params_hour)
    plot_time_series(ts, args.outdir, args.smooth,
                     title_suffix=f"{day_start.date()}")

    # -------- Hotspots (Monat = letzter Monatsteil, Jahr = global) --------
    hot_m = fetch_df(engine, HOT_SQL, {**params_month, "grain": "month"})
    hot_y = fetch_df(engine, HOT_SQL, {**params_year,  "grain": "year"})

    print("=== Hotspots (Monat, letzte Periode – Top 10) ===")
    if not hot_m.empty:
        lp = hot_m["period"].max()
        print(hot_m[hot_m["period"] == lp].head(10).to_string(index=False))
    print("\n=== Hotspots (Jahr, letzte Periode – Top 10) ===")
    if not hot_y.empty:
        lp = hot_y["period"].max()
        print(hot_y[hot_y["period"] == lp].head(10).to_string(index=False))

    plot_hotspots(hot_m, args.outdir, "monatlich")
    plot_hotspots(hot_y, args.outdir, "jährlich")

    # -------- Hail vs App (beide Fenster möglich; hier: global + Hinweis) --------
    hail_m = fetch_df(engine, HAIL_SQL, {**params_month, "grain": "month"})
    hail_y = fetch_df(engine, HAIL_SQL, {**params_year,  "grain": "year"})
    if hail_m.empty and hail_y.empty:
        print("\n(Hinweis) trip_type nicht vorhanden – keine Street-hail/App-Plots.")
    else:
        print("\nStreet-hail vs App – Daten vorhanden (Plots in diesem Skript nicht erzeugt).")

    # -------- Vendor (Monat = letzter Monatsteil, Jahr = global) --------
    vend_m = fetch_df(engine, VENDOR_SQL, {**params_month, "grain": "month"})
    vend_y = fetch_df(engine, VENDOR_SQL, {**params_year,  "grain": "year"})
    plot_vendor(vend_m, args.outdir, "monatlich")
    plot_vendor(vend_y, args.outdir, "jährlich")

    # -------- Rush & Tip (globales Fenster) --------
    rush = fetch_df(engine, RUSH_SQL, params_year)
    plot_rush_heatmap(rush, args.outdir, title_suffix="global")

    tip = fetch_df(engine, TIP_SQL, params_year)
    plot_tip(tip, args.outdir, title_suffix="global")

    # -------- Output --------
    print(f"\nFertig. Plots gespeichert in: {os.path.abspath(args.outdir)}")
    print("Dateien:")
    for fn in sorted(os.listdir(args.outdir)):
        print(" -", fn)


if __name__ == "__main__":
    main()
