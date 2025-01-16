
-- Merge dimension tables with codelist tables

DROP TABLE If EXISTS inr_measure_final;
CREATE TABLE inr_measure_final AS (
	SELECT im.*, cl.value AS full_name
	FROM int_rates_measure AS im 
		JOIN int_rates_cl_measure_codelist AS cl
		ON im.name = cl.code
);

DROP TABLE IF EXISTS bop_measure_final;
CREATE TABLE bop_measure_final AS (
	SELECT bm.*, cl.value AS full_name
	FROM bop_measure AS bm
		JOIN bop_cl_bop6_item_codelist cl
		ON bm.name = cl.code
);

DROP TABLE IF EXISTS entity_dimension_final;
CREATE TABLE entity_dimension_final AS (
	SELECT ed.*,
		   area_cl.value AS area_name,
		   curr_cl.value AS currency_name
	FROM entity_dimension_tbl AS ed 
		JOIN bop_cl_currency_codelist AS curr_cl
		ON ed.currency_code = curr_cl.code
		JOIN bop_cl_area_codelist AS area_cl
		ON ed.country_code = area_cl.code
);
DROP TABLE entity_dimension_tbl;