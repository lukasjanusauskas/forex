-- Dimension decisions:

-- Firstly, the decision about exchange rate type and frequency of measurement
-- has to be decided regardless of data.

-- - Frequency: daily
-- - Exchange rate type: I leave, for simplicity sake, only Spot(SP00 or 0) ex rates.
--   Other choices where effective exchange rates(NN00 and EN00).

WITH exr_types AS(
	SELECT DISTINCT("EXR_TYPE")
	FROM exchange_rates
		JOIN ex_rates_freq ON exchange_rates."FREQ" = ex_rates_freq.index
	WHERE ex_rates_freq.name = 'D'
)
SELECT name
FROM ex_rates_exr_type INNER JOIN
	exr_types ON ex_rates_exr_type.index = exr_types."EXR_TYPE";

-- We also have to drop the EXR_SUFFIX and CURRENCY_DENOM columns, since they are no longer relevant.

SELECT DISTINCT ON ("EXR_SUFFIX", "CURRENCY_DENOM") "EXR_SUFFIX", "CURRENCY_DENOM"
	FROM exchange_rates
		JOIN ex_rates_freq ON exchange_rates."FREQ" = ex_rates_freq.index
		INNER JOIN ex_rates_exr_type ON ex_rates_exr_type.index = exchange_rates."EXR_TYPE"
	WHERE ex_rates_freq.name = 'D' AND
		  ex_rates_exr_type.name = 'SP00';

-- Use hard-coded values, that I checked, for convenience

DELETE
FROM exchange_rates
WHERE exchange_rates."FREQ" <> (
	SELECT index
	FROM ex_rates_exr_type
	WHERE name = 'SP00'
) OR
	exchange_rates."EXR_TYPE" <> (
	SELECT index
	FROM ex_rates_freq
	WHERE name = 'D'
);

ALTER TABLE exchange_rates
DROP COLUMN "EXR_SUFFIX",
DROP COLUMN "EXR_TYPE",
DROP COLUMN "FREQ",
DROP COLUMN "CURRENCY_DENOM";

-- Check, where currency and unit is not the same thing

WITH units AS 
(
	SELECT name FROM ex_rates_unit
	EXCEPT
	SELECT name FROM ex_rates_currency
)
SELECT DISTINCT(ex_rates_currency.name)
FROM exchange_rates 
	JOIN ex_rates_unit
	ON exchange_rates."UNIT" = ex_rates_unit.index
	INNER JOIN units
	ON units.name = ex_rates_unit.name
	JOIN ex_rates_currency
	ON exchange_rates."CURRENCY" = ex_rates_currency.index;

WITH unique_counts AS (
	SELECT COUNT(*) OVER(PARTITION BY ("CURRENCY", "TIME_PERIOD") ) AS cnt
	FROM exchange_rates
)
SELECT COUNT(*) AS duplicates
FROM unique_counts
WHERE cnt > 1;

-- If we were to ommit all of the other rows, we would obtain other currencies' price against EURO
-- This is exactly, what we want, thus I will only keep CURRENCY and TIME_PERIOD

DROP TABLE IF EXISTS ex_rates;
CREATE TABLE ex_rates
AS
SELECT "CURRENCY" as currency,
	   "TIME_PERIOD" as time_period,
	   "OBS_VALUE" as rate
FROM exchange_rates;

DROP TABLE exchange_rates;
