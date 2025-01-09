CREATE MATERIALIZED VIEW monthly_data AS(
	WITH distinct_rates AS(
		SELECT DISTINCT ON (entity, date, ex_rate) 
			ent.currency_code AS entity, date, ex_rate
		FROM master JOIN entity_dimension_final AS ent
			ON master.entity = ent.index
		)
	SELECT r1.entity AS "entity1",
		   r2.entity AS "entity2",
		   r2.date,
		   r1.ex_rate / r2.ex_rate AS "ex_rate"
	FROM distinct_rates AS r1 
		JOIN distinct_rates AS r2 ON r1.date = r2.date
	ORDER BY EXTRACT(DAY FROM (CURRENT_DATE - r2.date) );
)

WITH distinct_rates AS(
	SELECT DISTINCT ON (entity, date, ex_rate) 
		ent.currency_code AS entity, date, ex_rate
	FROM master JOIN entity_dimension_final AS ent
		ON master.entity = ent.index
	)
SELECT r1.entity AS "entity1",
	   r2.entity AS "entity2",
	   r2.date,
	   r1.ex_rate / r2.ex_rate AS "ex_rate"
FROM distinct_rates AS r1 
	JOIN distinct_rates AS r2 ON r1.date = r2.date
ORDER BY EXTRACT(DAY FROM (CURRENT_DATE - r2.date) );

CREATE VIEW pairs AS (

)