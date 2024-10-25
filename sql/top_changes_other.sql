CREATE OR REPLACE FUNCTION lag_data(_tbl regclass) 
 RETURNS AS
$func$
	
$func$
LANGUAGE plpsql;

SELECT cpi."Country.code", "TIME_PERIOD", "CPI_TOT"
FROM cpi INNER JOIN non_euro
	ON cpi."Country.code" = non_euro."Country.code"
WHERE "CPI_TOT" IS NOT NULL;

WITH lagged AS
(SELECT "Country.Code", "TIME_PERIOD", "ppp",
 		LAG("ppp") OVER(PARTITION BY "Country.Code" ORDER BY "TIME_PERIOD") AS last_val
FROM ppp),
SELECT *
FROM lagged
ORDER BY ("ppp" - last_val) / "ppp"
FETCH FIRST 10 ROWS ONLY;