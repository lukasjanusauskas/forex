DROP FUNCTION lag_data;
CREATE OR REPLACE FUNCTION lag_data(_tbl regclass, col_name VARCHAR) 
 RETURNS TABLE (
	"Country.code" TEXT,
	"TIME_PERIOD" INT,
	obs_val DOUBLE PRECISION,
	obs_lag DOUBLE PRECISION
 ) AS
$$
	BEGIN RETURN QUERY
	EXECUTE
	format(	'SELECT "Country.code", "TIME_PERIOD", %s,
 				LAG(%s) OVER(PARTITION BY "Country.code" ORDER BY "TIME_PERIOD") AS last_val
			FROM %s', col_name, col_name, _tbl);
	END;
$$
LANGUAGE plpgsql;