-- Load Histogram function
CREATE OR REPLACE FUNCTION histogram(table_name_or_subquery text, column_name text)
RETURNS TABLE(bucket int, "range" numrange, freq bigint, bar text)
AS $func$
BEGIN
RETURN QUERY EXECUTE format('
  WITH
  source AS (
    SELECT * FROM %s
  ),
  min_max AS (
    SELECT min(%s) AS min, max(%s) AS max FROM source
  ),
  histogram AS (
    SELECT
      width_bucket(%s, min_max.min, min_max.max, 20) AS bucket,
      numrange(min(%s)::numeric, max(%s)::numeric, ''[]'') AS "range",
      count(%s) AS freq
    FROM source, min_max
    WHERE %s IS NOT NULL
    GROUP BY bucket
    ORDER BY bucket
  )
  SELECT
    bucket,
    "range",
    freq::bigint,
    repeat(''*'', (freq::float / (max(freq) over() + 1) * 15)::int) AS bar
  FROM histogram',
  table_name_or_subquery,
  column_name,
  column_name,
  column_name,
  column_name,
  column_name,
  column_name,
  column_name
  );
END
$func$ LANGUAGE plpgsql;

-- Pregunta 1
	SELECT 
		customer_id,
		AVG(time_between_rentals) AS average_time_between_rentals
	FROM
	(
		SELECT 
			customer_id,
			payment_date - LAG(payment_date)
				OVER (PARTITION BY customer_id ORDER BY payment_date) AS time_between_rentals
		FROM payment p
	) AS a
	GROUP BY customer_id;


-- Pregunta 2

-- NOTA: se divide entre 86400 para convertir la unidad de epoch a dias
SELECT histogram('(SELECT 
		customer_id,
		FLOOR(EXTRACT(epoch FROM AVG(time_between_rentals))/86400) AS average_time_between_rentals
	FROM
	(
		SELECT 
			customer_id,
			payment_date - LAG(payment_date)
				OVER (PARTITION BY customer_id ORDER BY payment_date) AS time_between_rentals
		FROM payment p
	) AS a
	GROUP BY customer_id) AS a','average_time_between_rentals');
-- Juzgando por el histograma, no sigue una distribución univarialmente normal.

-- Pregunta 3

SELECT
	stddev(average_time_between_rentals)/86400 AS desv_estan_dias_entre_rentas
FROM (
	SELECT
		EXTRACT(epoch FROM AVG(time_between_rentals)) AS average_time_between_rentals
	FROM
	(
		SELECT 
			customer_id,
			payment_date - LAG(payment_date)
				OVER (PARTITION BY customer_id ORDER BY payment_date) AS time_between_rentals
		FROM payment p
	) AS a
	GROUP BY customer_id
	) AS le_table;