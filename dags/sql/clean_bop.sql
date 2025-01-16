-- Dimensions:
-- - Entry: Balance(B). For exchange rate prediction we only want balance of the 
-- - Unit of measure: "XDC_USD" which stand for exchange rate converted US dollars
--					  (we need a unit, that would be common amongst all countries)
-- - Measures: goods, services and capital account
-- - Adjustment: not adjusted()

-- IMPORTANT: do not drop the CURRENCY column, 
-- as it is the only way we will be able to join with exchange rate relation

DROP TABLE IF EXISTS tmp_bop;
CREATE TEMPORARY TABLE tmp_bop AS
SELECT "REF_AREA" as entity,
	   "MEASURE" as measure,
	   "TIME_PERIOD" as date,
	   "OBS_VALUE" as value
	FROM balance_of_pay AS bop
		JOIN bop_freq AS freq ON bop."FREQ" = freq.index
		JOIN bop_unit_measure AS unit ON bop."UNIT_MEASURE" = unit.index 
		JOIN bop_accounting_entry AS ent ON bop."ACCOUNTING_ENTRY" = ent.index
		JOIN bop_adjustment AS adj ON bop."ADJUSTMENT" = adj.index
		JOIN bop_measure AS meas ON bop."MEASURE" = meas.index
			WHERE freq.name IN ('Q', 'A') AND
				  unit.name = 'USD_EXC' AND
				  ent.name = 'B' AND
				  adj.name = 'N' AND
				  meas.name IN ('G', 'S', 'CA');

-- currencies table:
-- currency_id: index of currency in balance_of_pay table(intermediate table)
-- entity_id: index of the country in balance_of_pay table

DROP TABLE IF EXISTS currencies;
CREATE TABLE currencies AS
(
	SELECT DISTINCT ON ("CURRENCY", "REF_AREA")
		"CURRENCY" AS currency_id, 
		"REF_AREA" AS entity_id
	FROM balance_of_pay 
		JOIN bop_currency ON balance_of_pay."CURRENCY" = bop_currency.index
	WHERE bop_currency.name <> '_Z'
);

DROP TABLE balance_of_pay;

CREATE TABLE balance_of_pay AS
SELECT * FROM tmp_bop;

DROP TABLE tmp_bop;