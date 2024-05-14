-- Common Table Expression (CTE) to retrieve bus positions data within the collected time frame
WITH BusPositions AS (
  SELECT
    VehicleID,
    TripID,
    Lat,
    Lon,
    Deviation,
    TIMESTAMP(DateTime) AS Timestamp
  FROM
    `inst767-project-big-data.bus_positions_dataset.bus_positions`
  WHERE
    TIMESTAMP(DateTime) >= TIMESTAMP("2024-04-18 00:00:00 UTC")
    AND TIMESTAMP(DateTime) <= TIMESTAMP("2024-04-26 23:59:59 UTC")
),
-- Common Table Expression (CTE) to retrieve stop locations data
Stops AS (
  SELECT
    stop_id,
    -- Convert stop coordinates to geographical point format
    ST_GEOGPOINT(stop_lon, stop_lat) AS stop_geo
  FROM
   `inst767-project-big-data.stops_dataset.stops_data`
)
-- Main query to join bus positions with stop locations and select relevant information

SELECT DISTINCT
  b.VehicleID,
  s.stop_id,
  b.TripID,
  b.Timestamp,
  b.Deviation,
  
FROM
  BusPositions b
JOIN
  Stops s
ON
  -- Join condition: check if the bus position is within a certain radius of the stop location
  ST_DWITHIN(ST_GEOGPOINT(b.Lon, b.Lat), s.stop_geo, 0.25 * 1609.34)
ORDER BY
  b.Timestamp, s.stop_id, b.VehicleID;


