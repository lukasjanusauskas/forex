-- Measures: long-term(IRLT), short-term(IRSTCI) and immediate(IR3TIB) interest rates
-- Other dimensions(other than ref_area and frequency) are simply not applicable(this will be illustrated later)
-- "REF_AREA" AS entity,
-- 		   "MEASURE" AS measure,
-- 		   "ADJUSTMENT" AS adjustment,
-- 		   "TIME_PERIOD" AS date,
-- 		   "OBS_VALUE" AS value
		   
DROP TABLE IF EXISTS tmp_int;
CREATE TEMPORARY TABLE tmp_int AS (
	SELECT "REF_AREA" AS entity,
		   "MEASURE" AS measure,
		   "ADJUSTMENT" AS adjustment,
		   "TIME_PERIOD" AS date,
		   "OBS_VALUE" AS value,
		   "UNIT_MULT", "ADJUSTMENT", "ACTIVITY"
	FROM interest_rate AS in_rate
		JOIN int_rates_measure AS meas ON in_rate."MEASURE" = meas.index
			WHERE meas.name IN ('IRLT', 'IRSTCI', 'IR3TIB')
);

-- Filtering rendered other dimensions redundand, thus we will be able to remove them
SELECT DISTINCT ON ("UNIT_MULT", "ADJUSTMENT", "ACTIVITY") "UNIT_MULT", "ADJUSTMENT", "ACTIVITY"
FROM tmp_int;

ALTER TABLE tmp_int
DROP COLUMN "UNIT_MULT",
DROP COLUMN "ADJUSTMENT",
DROP COLUMN "ACTIVITY";

-- Replace the temporary table with the new temporary table
DROP TABLE interest_rate;
CREATE TABLE interest_rates AS
SELECT * FROM tmp_int;
DROP TABLE tmp_int;
