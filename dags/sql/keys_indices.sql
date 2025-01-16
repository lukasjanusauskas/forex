
-- Creation of primary keys, unique indices and foreign keys

-- Unique indices for joining tables

ALTER TABLE currencies
ADD PRIMARY KEY (entity_id);

ALTER TABLE dim_currency
ADD PRIMARY KEY (ex_id);

CREATE UNIQUE INDEX bop_entity_index
ON dim_entity (bop_id);

CREATE UNIQUE INDEX inr_entity_index
ON dim_entity (int_id);

-- Unique indices for dimension tables

ALTER TABLE bop_measure_final
ADD PRIMARY KEY (index);

ALTER TABLE inr_measure_final
ADD PRIMARY KEY (index);

ALTER TABLE entity_dimension_final
ADD PRIMARY KEY (index);

CREATE UNIQUE INDEX ex_rate_index
ON entity_dimension_final (exr_index);

CREATE UNIQUE INDEX balance_of_pay_index
ON entity_dimension_final (bop_index);

-- Keys for relations
ALTER TABLE balance_of_pay
ADD PRIMARY KEY (entity, measure, date);

ALTER TABLE interest_rate
ADD PRIMARY KEY (entity, meas, date);

ALTER TABLE ex_rates
ADD PRIMARY KEY (currency, time_period)

