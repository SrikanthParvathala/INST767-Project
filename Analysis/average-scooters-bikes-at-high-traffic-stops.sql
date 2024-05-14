--Calculating the Average Number of Scooters and bikes at Stops
--Stop ids with bus count and average number of electric scooters and bikes to see if there are enough scooters at stops with high bus count

-- Defining a Common Table Expression (CTE) to calculate daily bus visits by stop and date
WITH DailyBusVisits AS (
  SELECT
    stop_id,
    DATE(Timestamp) AS Date, -- Extracting the date from the timestamp
    COUNT(DISTINCT VehicleID) AS BusCount -- Counting the distinct vehicles (buses) per stop and date
  FROM
    `inst767-project-big-data.aggregate_figures.bus-position-within-stop-unique`
  GROUP BY
    stop_id, Date --Grouping by stop and date to get daily bus counts
),

-- Defining another CTE to find the maximum bus count for each stop and date
MaxBusVisits AS (
  SELECT
    stop_id,
    Date,
    MAX(BusCount) AS MaxBusCount -- Calculating the maximum bus count for each stop and date
  FROM
    DailyBusVisits
  GROUP BY
    stop_id, Date -- Grouping by stop and date to find the maximum bus count
),
-- Creating a CTE to identify high-traffic stops based on the maximum daily bus count
HighTrafficStops AS (
  SELECT DISTINCT
  a.stop_id,
  a.Date,
  a.BusCount
FROM
  DailyBusVisits a
JOIN
  MaxBusVisits b ON a.Date = b.Date AND a.BusCount = b.MaxBusCount
  ORDER BY a.Date, a.BusCount DESC
)
-- Selecting stop_id, bus count, and average scooter count for high-traffic stops
SELECT
  f.stop_id,
  ht.BusCount,
  CAST(f.AvgScooters AS INT) AS AvgScooters -- Average scooter count (cast to integer)
FROM
  (
   -- Subquery to calculate the average scooter count per stop
    SELECT
      s.stop_id,
      AVG(s.total_count) AS AvgScooters
    FROM
      `inst767-project-big-data.aggregate_figures.scooter-count-per-stop` s
    JOIN
      HighTrafficStops h ON s.stop_id = h.stop_id
    GROUP BY
      s.stop_id
  )f
JOIN
  HighTrafficStops ht ON f.stop_id = ht.stop_id --Joining with high-traffic stops again
  ORDER BY  f.AvgScooters DESC, ht.BusCount DESC; -- Ordering the results by average scooter count and bus count in descending order


