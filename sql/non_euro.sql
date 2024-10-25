-- Countries, whose earnings are not measured by euros.
-- Will be helpfull to check, which countries never had euro as it's currency.
CREATE MATERIALIZED VIEW IF NOT EXISTS non_euro AS
SELECT DISTINCT "Country.code"
FROM earnings
WHERE "UNIT_MEASURE" <> 'EUR';

SELECT *
FROM non_euro
FETCH FIRST 5 ROWS ONLY; 