-- init.sql
CREATE TABLE IF NOT EXISTS rides_yellow (
  id                bigserial PRIMARY KEY,
  pickup_datetime   timestamptz,
  dropoff_datetime  timestamptz,
  trip_distance     double precision,
  fare_amount       numeric,
  tip_amount        numeric,
  total_amount      numeric,
  pu_loc            int,
  do_loc            int,
  vendor_id         int,
  passenger_count   int
);

CREATE TABLE IF NOT EXISTS rides_green (
  id                bigserial PRIMARY KEY,
  pickup_datetime   timestamptz,
  dropoff_datetime  timestamptz,
  trip_distance     double precision,
  fare_amount       numeric,
  tip_amount        numeric,
  total_amount      numeric,
  pu_loc            int,
  do_loc            int,
  vendor_id         int,
  trip_type         int,   -- green-spezifisch
  passenger_count   int
);

-- Indizes (pro Tabelle)
CREATE INDEX IF NOT EXISTS ix_rides_yellow_pickup ON rides_yellow(pickup_datetime);
CREATE INDEX IF NOT EXISTS ix_rides_yellow_pu     ON rides_yellow(pu_loc);
CREATE INDEX IF NOT EXISTS ix_rides_yellow_do     ON rides_yellow(do_loc);
CREATE INDEX IF NOT EXISTS ix_rides_yellow_vendor ON rides_yellow(vendor_id);

CREATE INDEX IF NOT EXISTS ix_rides_green_pickup  ON rides_green(pickup_datetime);
CREATE INDEX IF NOT EXISTS ix_rides_green_pu      ON rides_green(pu_loc);
CREATE INDEX IF NOT EXISTS ix_rides_green_do      ON rides_green(do_loc);
CREATE INDEX IF NOT EXISTS ix_rides_green_vendor  ON rides_green(vendor_id);


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