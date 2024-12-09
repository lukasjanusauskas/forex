-- There are no missing values because of the merge

-- However, there may still be big gaps between measurements, 
-- that may be some important events or missing data 

SELECT *
FROM master
ORDER BY entity, bop_measure, inr_measure, date;

WITH lagged_master AS (
	SELECT date - LAG(date) 
			OVER(ORDER BY entity, bop_measure, inr_measure, date) AS lag_measurements,
			entity,
			bop_measure,
			inr_measure,
			date
	FROM master
)
SELECT *
FROM lagged_master
WHERE EXTRACT(DAY FROM lag_measurements) > 5;

-- Turns out: there are no major missing data points.
-- However, there are many times, when the difference between measurements is 5 days, so I will delve deeper


WITH lagged_master AS (
	SELECT date - LAG(date) 
			OVER(ORDER BY entity, bop_measure, inr_measure, date) AS lag_measurements,
			entity,
			bop_measure,
			inr_measure,
			date
	FROM master
), entities_with_long_breaks AS (
	SELECT entity
	FROM lagged_master
	WHERE EXTRACT(DAY FROM lag_measurements) = 5
	GROUP BY entity
)
SELECT country_code
FROM entity_dimension JOIN entities_with_long_breaks AS ewlb
	ON entity_dimension.index = ewlb.entity;

WITH lagged_master AS (
	SELECT date - LAG(date) 
			OVER(ORDER BY entity, bop_measure, inr_measure, date) AS lag_measurements,
			entity,
			bop_measure,
			inr_measure,
			date
	FROM master
)
SELECT DISTINCT(date)
FROM lagged_master
WHERE EXTRACT(DAY FROM lag_measurements) = 5;

-- The 5 day breaks occur yearly in late march - early april.
