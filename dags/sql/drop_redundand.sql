CREATE OR REPLACE FUNCTION drop_tables(tbl_name TEXT) 
	RETURNS void AS
$$
BEGIN
	EXECUTE format('DROP TABLE IF EXISTS %s', tbl_name);
END;
$$ LANGUAGE plpgsql;

WITH delete_tables AS (
	SELECT table_name
	FROM information_schema.tables
	WHERE table_name ~ '(bop_)|(^int)|(ex_)|(balance_)|(currencies)' AND
		  table_name NOT IN ('master',
							  'bop_measure',
							  'int_rates_measure',
							  'entity_dimension')
)
SELECT drop_tables(table_name)
FROM delete_tables;
