-- Common Table Expression (CTE) to filter scooter data based on a data collection time frame
WITH
  FilteredScooters AS (
  SELECT
    bike_id,
    type,
    ST_GEOGPOINT(lon, lat) AS geo_point,
    DATE(timestamp_EST) AS date,
    EXTRACT(HOUR
    FROM
      timestamp_EST) AS hour,
    EXTRACT(MINUTE
    FROM
      timestamp_EST) AS minute
  FROM
    `inst767-project-big-data.scooter_dataset.vehicle_usage_table_UPDATED`
  WHERE
    timestamp_EST >= TIMESTAMP("2024-04-18 00:00:00 UTC")
    AND timestamp_EST <= TIMESTAMP("2024-04-26 23:59:59 UTC")),
-- Common Table Expression (CTE) to precompute stop locations
  PreComputedStops AS (
  SELECT
    stop_id,
    ST_GEOGPOINT(stop_lon, stop_lat) AS stop_geo
  FROM
    `inst767-project-big-data.stops_dataset.stops_data` )
-- Main query to join scooter data with precomputed stop locations and aggregate counts

SELECT
  p.stop_id,
  s.date,
  s.hour,
  s.minute,
  COUNT(s.bike_id) AS total_count,
  COUNTIF(s.type = 'electric_bike') AS count_electric_bikes,
  COUNTIF(s.type = 'electric_scooter') AS count_electric_scooters
FROM
  PreComputedStops AS p
JOIN
  FilteredScooters AS s
ON
  ST_DWITHIN(s.geo_point, p.stop_geo, 0.25 * 1609.34) -- Join condition: check if scooter is within a certain radius of the stop
GROUP BY
  p.stop_id,
  s.date,
  s.hour,
  s.minute