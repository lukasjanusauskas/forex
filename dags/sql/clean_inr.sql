-- Measures: long-term(IRLT), short-term(IRSTCI) and immediate(IR3TIB) interest rates
-- Other dimensions(other than ref_area and frequency) are simply not applicable(this will be illustrated later)
-- "REF_AREA" AS entity,
-- 		   "MEASURE" AS measure,
-- 		   "ADJUSTMENT" AS adjustment,
-- 		   "TIME_PERIOD" AS date,
-- 		   "OBS_VALUE" AS value
		   
DROP TABLE IF EXISTS tmp_int;
CREATE TEMPORARY TABLE tmp_int AS (
	SELECT in_rate.*
	FROM interest_rate AS in_rate
		JOIN int_rates_measure AS meas ON in_rate."MEASURE" = meas.index
		JOIN int_rates_freq AS freq ON in_rate."FREQ" = freq.index
			WHERE meas.name IN ('IRLT', 'IRSTCI', 'IR3TIB')
				  AND freq.name = 'Q'
);

WITH non_unique_dims AS (
	SELECT "REF_AREA", "MEASURE", "TIME_PERIOD"
	FROM tmp_int
	GROUP BY "REF_AREA", "MEASURE", "TIME_PERIOD"
	HAVING COUNT(*) > 1
)
SELECT inr.*
FROM interest_rate AS inr
	JOIN non_unique_dims AS nud ON inr."REF_AREA" = nud."REF_AREA"
		AND inr."MEASURE" = nud."MEASURE"
		AND inr."TIME_PERIOD" = nud."TIME_PERIOD"
ORDER BY inr."TIME_PERIOD";

-- Replace the interest rate table with the new temporary table
DROP TABLE interest_rate;

CREATE TABLE interest_rate AS
SELECT "REF_AREA" as entity,
	   "MEASURE" as meas,
	   "TIME_PERIOD" as date,
	   "OBS_VALUE" as value
FROM tmp_int;

DROP TABLE tmp_int;