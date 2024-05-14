--Stops with highest bus count

WITH DailyBusVisits AS (
  SELECT
    stop_id,
    DATE(Timestamp) AS Date,
    COUNT(DISTINCT VehicleID) AS BusCount
  FROM
    `inst767-project-big-data.aggregate_figures.bus-position-within-stop-unique`
  GROUP BY
    stop_id, Date
),
MaxBusVisits AS (
  SELECT
    stop_id,
    Date,
    MAX(BusCount) AS MaxBusCount
  FROM
    DailyBusVisits
  GROUP BY
    stop_id, Date
)
SELECT DISTINCT
  a.stop_id,
  a.Date,
  a.BusCount
FROM
  DailyBusVisits a
JOIN
  MaxBusVisits b ON a.Date = b.Date AND a.BusCount = b.MaxBusCount
  ORDER BY a.Date, a.BusCount DESC;