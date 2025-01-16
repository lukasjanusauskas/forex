-- For joining we must refer to the dimension tables
-- First, we will remove any incosistencies and then merge dimension tables

-- Select entity codes, that occur in both tables
-- and put all of the data into a single relation(fact table)


-- Select currencies present in both ex_rates and balance_of_pay
DROP TABLE IF EXISTS dim_currency;
CREATE TABLE dim_currency
AS (
	SELECT ex_rates_currency.name, 
		   bop_currency.index AS bop_id,
		   ex_rates_currency.index AS ex_id
	FROM ex_rates_currency LEFT JOIN bop_currency
		ON bop_currency.name = ex_rates_currency.name
);

-- Select entities, that appear at both interest_rate and balance_of_pay 
DROP TABLE IF EXISTS dim_entity;
CREATE TABLE dim_entity
AS (
	SELECT bop_ref_area.name, 
		   bop_ref_area.index * 1000 + int_rates_ref_area.index AS index,
		   bop_ref_area.index AS bop_id,
		   int_rates_ref_area.index AS int_id
	FROM bop_ref_area JOIN int_rates_ref_area
		   ON bop_ref_area.name = int_rates_ref_area.name
);

-- Delete euro countries
WITH euro_index AS (
	SELECT currency_id AS curr_id
	FROM currencies
	GROUP BY currency_id
	HAVING COUNT(*) > 1
)
DELETE FROM dim_entity
WHERE bop_id IN 
(
	SELECT index
	FROM bop_ref_area JOIN currencies
		ON bop_ref_area.index = currencies.entity_id,
		euro_index
	WHERE currencies.currency_id = euro_index.curr_id
		AND bop_ref_area.name <> 'EU27_2020'
);

-- Create entity dimension table
DROP TABLE IF EXISTS entity_dimension_tbl;
CREATE TABLE entity_dimension_tbl AS (
	SELECT dim_entity.index AS index,
		   dim_currency.ex_id AS exr_index,
		   dim_entity.bop_id AS bop_index,
		   dim_currency.name AS currency_code,
		   dim_entity.name AS country_code
	FROM dim_currency 
		 JOIN currencies ON dim_currency.bop_id = currencies.currency_id
		 FULL JOIN dim_entity ON dim_entity.bop_id = currencies.entity_id
);

-- Create temporary fact table
DROP MATERIALIZED VIEW IF EXISTS master;
CREATE MATERIALIZED VIEW master AS
(
	SELECT ex_rates.currency,
			bop.measure AS bop_measure,
			inr.meas AS inr_measure,
			ex_rates.time_period AS date,
			bop.value AS bop_value,
			inr.value AS inr_value,
		   ex_rates.rate AS rate
	FROM ex_rates 
		JOIN dim_currency ON ex_rates.currency = dim_currency.ex_id 
			AND dim_currency.bop_id IS NOT NULL
		JOIN currencies AS curr ON curr.currency_id = dim_currency.bop_id
		LEFT JOIN balance_of_pay AS bop ON bop.entity = curr.entity_id
				AND EXTRACT( MONTH FROM ex_rates.time_period ) - EXTRACT(MONTH FROM bop.date) < 3
				AND ex_rates.time_period > bop.date
				AND EXTRACT( YEAR FROM ex_rates.time_period) = EXTRACT(YEAR FROM bop.date)
		LEFT JOIN dim_entity ON dim_entity.bop_id = bop.entity
		LEFT JOIN interest_rate AS inr ON inr.entity = dim_entity.int_id
			AND inr.date = bop.date
	ORDER BY date DESC
);

DROP MATERIALIZED VIEW IF EXISTS ex_pairs;
CREATE MATERIALIZED VIEW ex_pairs AS (
	SELECT ex1.currency AS currency_1,
		   ex2.currency as currency_2,
		   ex1.time_period AS date,
		   ex2.rate / ex1.rate AS rate
	FROM ex_rates AS ex1 JOIN ex_rates AS ex2
		ON ex1.currency > ex2.currency AND
			ex1.time_period = ex2.time_period
);

DROP MATERIALIZED VIEW IF EXISTS macro;
CREATE MATERIALIZED VIEW macro AS (
	SELECT dim_entity.index,
		   bop.measure,
		   inr.meas AS inr_measure,
		   bop.date AS date,
		   bop.value AS bop_value,
		   inr.value AS inr_value
		FROM balance_of_pay AS bop JOIN dim_entity ON dim_entity.bop_id = bop.entity
			LEFT JOIN interest_rate AS inr ON inr.entity = dim_entity.int_id
				AND inr.date = bop.date
);