-- Check, whether updates aren't empty

DO $$
DECLARE 
	relevant_rows integer;

BEGIN
	SELECT COUNT(*)
	INTO relevant_rows
	FROM updates
	WHERE "FREQ" = 'D' AND
		  "EXR_TYPE" = 'SP00';

	ASSERT relevant_rows > 0, 'Values have not been updated yet or the dimension codes have changed';
END$$;

-- Check, whether UNIT_MULT hasn't changed(from 0)

DO $$
DECLARE 
	distinct_unit_mults integer;

BEGIN
	SELECT COUNT(DISTINCT("UNIT_MULT"))
	INTO distinct_unit_mults
	FROM updates;

	ASSERT distinct_unit_mults = 1, 'There is a change in data: UNIT_MULT has unseen(non-zero) value';
END$$;

-- Check, whether, as in the past, the other data dimensions, which haven't been used do not have more than one distinct value each
-- Meaning, we need to check, whether selecting by exchange rate type and frequency is enough to not obtain duplicates

DO $$
DECLARE 
	distinct_secondary_dims integer;

BEGIN
	SELECT COUNT(DISTINCT("EXR_SUFFIX", "CURRENCY_DENOM"))
	INTO distinct_secondary_dims
	FROM updates
	WHERE "FREQ" = 'D' AND
		  "EXR_TYPE" = 'SP00';

	ASSERT distinct_secondary_dims = 1, 'There is a change in data: one of "EXR_SUFFIX", "CURRENCY_DENOM" have multiple distinct values';
END$$;

CREATE TEMPORARY TABLE tmp_ex
AS 
(
	SELECT dim_curr.ex_id AS currency,
		   "TIME_PERIOD" AS time_period,
		   "OBS_VALUE" AS rate
	FROM updates 
		FULL JOIN dim_currency AS dim_curr
		ON updates."CURRENCY" = dim_curr.name
	WHERE "FREQ" = 'D' AND
		  "EXR_TYPE" = 'SP00'
);

-- Merge the names of the currencies and check if there are no new ones

DO
$$
DECLARE
	number_na_ids integer;
BEGIN
	SELECT COUNT(*)
	INTO number_na_ids
	FROM tmp_ex
	WHERE currency IS NULL;

	ASSERT number_na_ids = 0, "There are some new currencies or renamed currencies";
END
$$;

-- Insert updated values

WITH update_date AS (
	SELECT MAX(time_period) AS date
	FROM ex_rates
)
INSERT INTO ex_rates
SELECT tmp_ex.*
FROM tmp_ex, update_date
WHERE tmp_ex.time_period > date;

DROP TABLE updates;

