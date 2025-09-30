-- init.sql
CREATE TABLE IF NOT EXISTS rides (
  id                bigserial PRIMARY KEY,
  service_type      text,                 -- 'yellow' / 'green'
  pickup_datetime   timestamptz,
  dropoff_datetime  timestamptz,
  trip_distance     double precision,
  fare_amount       numeric,
  tip_amount        numeric,
  total_amount      numeric,
  pu_loc            int,
  do_loc            int,
  vendor_id         int,
  trip_type         int,                  -- green: 1=street-hail, 2=dispatch (app), sonst NULL
  passenger_count   int
);

CREATE INDEX IF NOT EXISTS ix_rides_pickup   ON rides(pickup_datetime);
CREATE INDEX IF NOT EXISTS ix_rides_service  ON rides(service_type);
CREATE INDEX IF NOT EXISTS ix_rides_pu       ON rides(pu_loc);
CREATE INDEX IF NOT EXISTS ix_rides_do       ON rides(do_loc);
CREATE INDEX IF NOT EXISTS ix_rides_vendor   ON rides(vendor_id);


-- NEU: Lookup-Tabelle
CREATE TABLE IF NOT EXISTS taxi_zones (
  "LocationID"   int PRIMARY KEY,
  "Borough"      text,
  "Zone"         text,
  "service_zone" text
);

-- NEU: CSV importieren (läuft automatisch beim ersten DB-Start)
-- Pfad zeigt auf den gemounteten init-Ordner im Container:
COPY taxi_zones ("LocationID","Borough","Zone","service_zone")
FROM '/docker-entrypoint-initdb.d/taxi_zone_lookup.csv'
WITH (FORMAT csv, HEADER true);