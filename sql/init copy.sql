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
  do_loc            int
);
CREATE INDEX IF NOT EXISTS ix_rides_pickup   ON rides(pickup_datetime);
CREATE INDEX IF NOT EXISTS ix_rides_service  ON rides(service_type);
