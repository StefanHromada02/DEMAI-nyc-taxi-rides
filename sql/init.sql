-- Rides-Tabellen (mit ingested_at)
-- Yellow (minimal)
CREATE TABLE IF NOT EXISTS public.rides_yellow (
  id               bigserial PRIMARY KEY,
  pickup_datetime  timestamptz,
  dropoff_datetime timestamptz,
  pu_loc           int,
  do_loc           int,
  passenger_count  int,
  payment_type     int,
  ingested_at      timestamptz NOT NULL DEFAULT now()
);

-- Green (minimal)
CREATE TABLE IF NOT EXISTS public.rides_green (
  id               bigserial PRIMARY KEY,
  pickup_datetime  timestamptz,
  dropoff_datetime timestamptz,
  pu_loc           int,
  do_loc           int,
  passenger_count  int,
  trip_type        int,
  payment_type     int,
  ingested_at      timestamptz NOT NULL DEFAULT now()
);

-- Indizes passend zum Query-Profil
CREATE INDEX IF NOT EXISTS ix_rides_yellow_pickup   ON public.rides_yellow (pickup_datetime);
CREATE INDEX IF NOT EXISTS ix_rides_yellow_pu       ON public.rides_yellow (pu_loc);
CREATE INDEX IF NOT EXISTS ix_rides_yellow_do       ON public.rides_yellow (do_loc);
CREATE INDEX IF NOT EXISTS ix_rides_yellow_ingested ON public.rides_yellow (ingested_at DESC);

CREATE INDEX IF NOT EXISTS ix_rides_green_pickup    ON public.rides_green (pickup_datetime);
CREATE INDEX IF NOT EXISTS ix_rides_green_pu        ON public.rides_green (pu_loc);
CREATE INDEX IF NOT EXISTS ix_rides_green_do        ON public.rides_green (do_loc);
CREATE INDEX IF NOT EXISTS ix_rides_green_ingested  ON public.rides_green (ingested_at DESC);

-- Taxi-Zonen Lookup
CREATE TABLE IF NOT EXISTS taxi_zones (
  "LocationID"   int PRIMARY KEY,
  "Borough"      text,
  "Zone"         text,
  "service_zone" text
);

COPY taxi_zones ("LocationID","Borough","Zone","service_zone")
FROM '/docker-entrypoint-initdb.d/taxi_zone_lookup.csv'
WITH (FORMAT csv, HEADER true);

-- Ingest-Stats (optional, wie gehabt)
CREATE TABLE IF NOT EXISTS public.ingest_stats (
  batch_id      BIGINT,
  service_type  TEXT,
  rows_total    BIGINT,
  rows_null_pickup BIGINT,
  rows_null_dropoff BIGINT,
  rows_inversed BIGINT,
  rows_equal_ts BIGINT,
  rows_dupes    BIGINT,
  pickup_min    TIMESTAMP,
  pickup_max    TIMESTAMP,
  created_at    TIMESTAMP DEFAULT now()
);

-- Vendor-Provider Lookup (optional)
CREATE TABLE IF NOT EXISTS public.vendor_providers (
  vendor_id   int PRIMARY KEY,
  provider    text NOT NULL,
  note        text
);

INSERT INTO public.vendor_providers (vendor_id, provider) VALUES
  (1, 'Creative Mobile Technologies, LLC'),
  (2, 'Curb Mobility, LLC'),
  (6, 'Myle Technologies Inc'),
  (7, 'Helix')
ON CONFLICT (vendor_id) DO NOTHING;

-- LISTEN/NOTIFY Trigger
CREATE OR REPLACE FUNCTION public.notify_new_ride()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  payload jsonb;
BEGIN
  payload := jsonb_build_object(
    'table', TG_TABLE_NAME,
    'id', NEW.id,
    'pickup_datetime', NEW.pickup_datetime,
    'ingested_at', NEW.ingested_at
  );
  PERFORM pg_notify('rides_new', payload::text);
  RETURN NEW;
END;
$$;

-- Trigger YELLOW
DROP TRIGGER IF EXISTS trg_notify_new_ride_y ON public.rides_yellow;
CREATE TRIGGER trg_notify_new_ride_y
AFTER INSERT ON public.rides_yellow
FOR EACH ROW EXECUTE FUNCTION public.notify_new_ride();

-- Trigger GREEN
DROP TRIGGER IF EXISTS trg_notify_new_ride_g ON public.rides_green;
CREATE TRIGGER trg_notify_new_ride_g
AFTER INSERT ON public.rides_green
FOR EACH ROW EXECUTE FUNCTION public.notify_new_ride();

-- explizit aktivieren (falls mal disabled)
ALTER TABLE public.rides_yellow ENABLE TRIGGER trg_notify_new_ride_y;
ALTER TABLE public.rides_green  ENABLE TRIGGER trg_notify_new_ride_g;
