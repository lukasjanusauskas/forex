-- For joining we must refer to the dimension tables
-- First, we will remove any incosistencies and then merge dimension tables

-- Select entity codes, that occur in both tables
-- and put all of the data into a single relation(fact table)


-- Select currencies present in both ex_rates and balance_of_pay
DROP TABLE IF EXISTS dim_currency;
CREATE TEMPORARY TABLE dim_currency
AS (
	SELECT bop_currency.name, 
		   bop_currency.index AS bop_id,
		   ex_rates_currency.index AS ex_id
	FROM bop_currency JOIN ex_rates_currency
		ON bop_currency.name = ex_rates_currency.name
);

-- Select entities, that appear at both interest_rate and balance_of_pay 
DROP TABLE IF EXISTS dim_entity;
CREATE TEMPORARY TABLE dim_entity
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

-- Merge and attempt to create foreign keys

-- Check, that the merge is any good(whether it doesn't or does have duplicates)
SELECT bop.entity, bop.measure, bop.date, inr.meas
FROM balance_of_pay AS bop JOIN interest_rate AS inr
	ON bop.entity = inr.entity 
		AND bop.date = inr.date
GROUP BY bop.entity, bop.measure, bop.date, inr.meas
HAVING COUNT(*) > 1;

-- Create temporary fact table
DROP TABLE IF EXISTS tmp_master;
CREATE TEMPORARY TABLE tmp_master AS
(
	SELECT dim_entity.index AS entity,
		   bop.measure AS bop_measure,
		   inr.meas AS measure,
		   ex_rates.time_period AS date,
		   bop.value AS bop_value,
		   inr.value AS interest_rate,
		   ex_rates.rate AS ex_rate
	FROM balance_of_pay AS bop JOIN currencies AS curr	
			ON bop.entity = curr.entity_id
			JOIN dim_currency ON curr.currency_id = dim_currency.ex_id
			JOIN ex_rates ON dim_currency.ex_id = ex_rates.currency
				AND EXTRACT(QUARTER FROM ex_rates.time_period) = EXTRACT(QUARTER FROM bop.date)
				AND EXTRACT(YEAR FROM ex_rates.time_period) = EXTRACT(YEAR FROM bop.date)
			JOIN dim_entity ON dim_entity.bop_id = bop.entity
			JOIN interest_rate AS inr ON inr.entity = dim_entity.int_id
				 AND inr.date = bop.date
);

-- Create entity dimension table
DROP TABLE IF EXISTS entity_dimension_tbl;
CREATE TEMPORARY TABLE entity_dimension_tbl AS (
	SELECT dim_entity.index AS index,
		   dim_currency.name AS currency_code,
		   dim_entity.name AS country_code
	FROM dim_currency 
		 JOIN currencies ON dim_currency.bop_id = currencies.currency_id
		 JOIN dim_entity ON dim_entity.bop_id = currencies.entity_id
);

-- Run experiments: if constraints cannot be created - something isn't correct
-- If such errors occur, temporary tables couldn't be turned into actual tables

-- Dimension table primary key creation
ALTER TABLE entity_dimension_tbl
ADD PRIMARY KEY (index);

-- Foreign key for entity
ALTER TABLE tmp_master
ADD CONSTRAINT entity_fk
FOREIGN KEY (entity) REFERENCES entity_dimension_tbl (index);

-- There is no way to test, whether we will be able to create foreign keys for bop and inr measures
CREATE TABLE master AS 
SELECT * FROM tmp_master;

DROP TABLE tmp_master;

CREATE TABLE entity_dimension AS 
SELECT * FROM entity_dimension_tbl;

DROP TABLE entity_dimension_tbl;

-- Create primary and foreign keys
-- Primary key for entity dimension table

ALTER TABLE entity_dimension
ADD PRIMARY KEY (index);

-- Foreign key for entity

ALTER TABLE master
ADD CONSTRAINT entity_fk
FOREIGN KEY (entity) REFERENCES entity_dimension (index); 

-- Primary key for measure dimension tables

ALTER TABLE bop_measure
ADD PRIMARY KEY (index);

ALTER TABLE int_rates_measure
ADD PRIMARY KEY (index);

-- Foreign keys for measure dimension tables

ALTER TABLE master
ADD CONSTRAINT bop_measure_fk
FOREIGN KEY (bop_measure) REFERENCES bop_measure (index); 

ALTER TABLE master
RENAME COLUMN measure TO inr_measure;

ALTER TABLE master
ADD CONSTRAINT inr_measure_fk
FOREIGN KEY (inr_measure) REFERENCES int_rates_measure (index); 

-- Primary key for the fact table

ALTER TABLE master
ADD PRIMARY KEY (entity, date, bop_measure, inr_measure);
