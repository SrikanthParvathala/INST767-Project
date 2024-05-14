-- Query Start --- 

WITH get_avg_scooters_for_each_hour_and_date AS (
SELECT stop_id, date, hour, CAST(FLOOR(AVG(total_count)) AS INT) AS avg_scooters_during_that_hour
FROM `inst767-project-big-data.aggregate_figures.scooter-count-per-stop`
WHERE date > '2024-04-18'
GROUP BY 1,2,3
ORDER BY 1,2,3),

difference_cte AS (
SELECT *, LAG(avg_scooters_during_that_hour,1,0) OVER(PARTITION BY stop_id ORDER BY date, hour ASC) AS previous_scooter_count_during_that_hour, 
(LAG(avg_scooters_during_that_hour,1,0) OVER(PARTITION BY stop_id ORDER BY date, hour ASC) - avg_scooters_during_that_hour) AS number_of_scooters_used
FROM get_avg_scooters_for_each_hour_and_date
ORDER BY stop_id, date, hour),

scooters_used_per_day_1 AS (
SELECT stop_id,hour, CAST(FLOOR(AVG(number_of_scooters_used)) AS INT) AS avg_scooters_used_per_day_for_this_hour
FROM difference_cte
GROUP BY 1,2
ORDER BY 1,2),

ranking_cte AS (
SELECT *, DENSE_RANK() OVER(PARTITION BY stop_id ORDER BY avg_scooters_used_per_day_for_this_hour DESC) AS rnk
FROM scooters_used_per_day_1
ORDER BY stop_id, hour),

last_cte AS (SELECT stop_id, CONCAT('Between ', hour, ' and ', hour+1) AS time_period, avg_scooters_used_per_day_for_this_hour AS avg_scooters_used_per_day_between_this_time_period_at_this_stop
FROM ranking_cte WHERE rnk = 1
ORDER BY 3 DESC)

SELECT time_period, SUM(avg_scooters_used_per_day_between_this_time_period_at_this_stop) AS total_number_of_scooters_used_during_this_rush_hour FROM last_cte
GROUP BY 1
ORDER BY 2 DESC;

-- Explanation of the Analysis Query -- 
-- First CTE, we get stop ID, time and the average of the number of scooters during that hour. This is done because there is also a 'minute' column due to which we cannot aggregate directly, since there are several rows containing the same hour but different minute values, and hence the aggregation gets exaggerated

-- In the 2nd CTE, we take the average number of scooters and also get the avg number of scooters for the previous hour. There is also a column at the end which gives the difference between the number of scooters before_and_after. If the value is Negative, then more number of scooters were taken by people before this particular hour. With this, we have the data for the stop id, day, hour & difference in number of scooters between 2 hours.

--Now, in next cte, we take the average of the number of scooters that were used to find out the 'GENERAL' rush hour for each stop id, irrespective of the date. Because, on 1 day, rush hour could be at 5 PM. The next day, it could be at 4 PM, so to account for such situation, we just take the generalized average for each stop id for each hour.

--In the ranking_cte, we assign a rank to each row. For each stop id, if the avg.number of scooters that were used is the highest, it will be assigned the rank '1'

-- In the last_cte, we only filter out those rows which have the rank as 1, since they will represent the hour during which the average number of scooters that were used, were highest.

-- If we run the query only upto this point, we get the stop_id, and the hour for that specific stop_id which is the rush hour, along with the avg. scooters used per day during this rush hour.

-- Lastly, the query at the end will give the total number of scooters used during that particular rush hour
