--Average number of scooters available in a specific bus stop during peak hours

SELECT
  scooter.stop_id,
  CAST(AVG(count_electric_bikes) AS INT) AS AverageElectricBikes,
  CAST(AVG(count_electric_scooters) AS INT) AS AverageElectricScooters,
  CAST(AVG(total_count) AS INT) AS AverageBikes
FROM
  `inst767-project-big-data.aggregate_figures.scooter-count-per-stop` as scooter
WHERE
  hour BETWEEN 18 AND 19  --Peak hour was analysed in the 'Finding out Rush Hours for Each Stop ID' Query and output was which is 18 (range between hour 18-19)
GROUP BY
  scooter.stop_id
ORDER BY AverageBikes DESC;
