CREATE OR REPLACE FUNCTION drop_tables(tbl_name TEXT) 
	RETURNS void AS
$$
BEGIN
	EXECUTE format('DROP TABLE IF EXISTS "%s"', tbl_name);
END;
$$ LANGUAGE plpgsql;
SELECT drop_tables(table_name)
FROM information_schema.tables
WHERE table_name ~ '(bop_)|(^int)|(exr?_)|(balance_)|(currencies)|(^exch)' AND
	  table_name NOT IN ('master',
						 'bop_measure',
						 'int_rates_measure',
						 'entity_dimension')
