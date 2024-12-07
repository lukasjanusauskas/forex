-- For joining we must refer to the dimension tables
-- First, we will remove any incosistencies and then merge dimension tables

-- Select entity codes, that occur in both tables
-- and put all of the data into a single relation(fact table)


-- Select currencies present in both ex_rates and balance_of_pay
DROP TABLE dim_currency;
CREATE TEMPORARY TABLE dim_currency
AS (
	WITH max_ex AS (
		SELECT MAX(ex_rates_currency.index) AS max_ex
		FROM ex_rates_currency
	)
	SELECT bop_currency.name, 
		   bop_currency.index * max_ex.max_ex + ex_rates_currency.index AS index,
		   bop_currency.index AS bop_id,
		   ex_rates_currency.index AS ex_id
	FROM bop_currency JOIN ex_rates_currency
		ON bop_currency.name = ex_rates_currency.name,
		max_ex
);

-- Select entities, that appear at both interest_rate and balance_of_pay 
DROP TABLE dim_currency;
CREATE TEMPORARY TABLE dim_currency
AS (
	WITH max_int AS (
		SELECT MAX(int_rates_ref_area.index) AS max_int
		FROM int_rates_ref_area
	)
	SELECT bop_ref_area.name, 
		   bop_ref_area.index * max_int.max_int + int_rates_ref_area.index AS index,
		   bop_ref_area.index AS bop_id,
		   int_rates_ref_area.index AS int_id
	FROM bop_ref_area JOIN int_rates_ref_area
		   ON bop_ref_area.name = int_rates_ref_area.name,
		   max_int
);

-- Select currencies that have an unique currency

-- Currency, that has many entities: Euro.
-- In tables from OECD I will only leave a single entity(EU27_2020)
WITH euro_index AS (
	SELECT currency_id
	FROM currencies
	GROUP BY currency_id
	HAVING COUNT(*) > 1
)
SELECT bop_ref_area.name
FROM currencies 
	JOIN euro_index	 ON currencies.currency_id = euro_index.currency_id
	JOIN bop_ref_area ON bop_ref_area.index = currencies.entity_id;

-- Update indices in the relations to be able to merge


