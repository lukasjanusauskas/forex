CREATE OR REPLACE FUNCTION drop_tables(tbl_name TEXT) 
	RETURNS void AS
$$
BEGIN
	EXECUTE format('DROP TABLE IF EXISTS "%s"', tbl_name);
END;
$$ LANGUAGE plpgsql;

SELECT drop_tables(table_name)
FROM information_schema.tables
WHERE table_name ~ '(bop_)|(int_)|(exr?_)|(exch)'
	AND NOT table_name ~ 'final$'
	AND table_name <> 'ex_rates';

SELECT drop_tables(table_name)
FROM information_schema.tables
WHERE table_name ~ 'cl' AND
	NOT table_name ~ '^pg';