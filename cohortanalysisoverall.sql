SELECT 
  country,
  transaction_month,
  cohort_month,
  count(distinct(profile_id)) as User_count,
  sum(transaction_count) as no_of_transaction, 
  sum(transaction_value) as Total_Value
FROM 
(
WITH
	conversion_0 AS (
		SELECT
			LSQ.conversion_table_id AS conversion_id,
			RSQ.source_currency,
			RSQ.target
		FROM
			`tvlk-data-mkt-prod`.datamart.conversion_table AS LSQ
				CROSS JOIN
			UNNEST( conversion_data ) AS RSQ
	),
	conversion_1 AS (
			SELECT
				LSQ.conversion_id,
				LSQ.source_currency,
				RSQ.currency AS target_currency,
				RSQ.exchange_rates
			FROM
				conversion_0 AS LSQ
					CROSS JOIN
				UNNEST( target ) AS RSQ
			WHERE
				RSQ.currency = 'IDR'
	),
	sales_0 AS (
		SELECT
			profile_id,
			booking_id,
			conversion_table_id AS conversion_id,
			invoice_currency,
			invoice_amount,
			payment_method,
			last_payment_assignment_timestamp AS payment_time,
      locale,
			ROW_NUMBER()
				OVER(
					PARTITION BY
						booking_id
					ORDER BY
						last_payment_assignment_timestamp
				)
				AS record_sequence
		FROM
			`tvlk-data-mkt-prod`.datamart.sales_table
		WHERE
			--_PARTITIONTIME <= TIMESTAMP( '2019-05-31' )
			--	AND
			profile_id IS NOT NULL
				AND
			booking_status = 'ISSUED'
			--	AND
			--locale LIKE '%_SG'
	),
	sales_1 AS (
		SELECT
			LSQ.profile_id,
			LSQ.booking_id,
			LSQ.invoice_amount * IF( RSQ.exchange_rates IS NULL AND LSQ.invoice_currency = 'IDR', 1, RSQ.exchange_rates ) AS invoice_amount,
			payment_method,
      SUBSTR(locale,-2) as country,
			DATE( TIMESTAMP_TRUNC( TIMESTAMP_ADD( TIMESTAMP_MILLIS( LSQ.payment_time ), INTERVAL 7 HOUR ), MONTH ) ) AS transaction_month
		FROM
			sales_0 AS LSQ
				LEFT JOIN
			conversion_1 AS RSQ
				ON
					LSQ.conversion_id = RSQ.conversion_id
						AND
					LSQ.invoice_currency = RSQ.source_currency
		WHERE
			LSQ.record_sequence = 1
	),
	sales_2 AS (
		SELECT
			MIN( transaction_month )
				OVER(
					PARTITION BY
						profile_id
				)
				AS cohort_month,
			profile_id,
			booking_id,
			invoice_amount,
			payment_method,
      country,
			transaction_month
		FROM
			sales_1
		--WHERE
		--	transaction_month <= DATE( '2019-05-31' )
	),
	sales_3 AS (
		SELECT
			profile_id,
			cohort_month,
			transaction_month,
      country,
			COUNT( booking_id ) AS transaction_count,
			SUM( invoice_amount ) AS transaction_value
		FROM
			sales_2
		GROUP BY
			1,
			2,
			3,
			4
		ORDER BY
			1,
			2,
			3,
			4
	),
	sales_4 AS (
		SELECT
			profile_id,
			cohort_month,
			transaction_month,
      country,
			COUNT( booking_id ) AS transaction_count,
			SUM( invoice_amount ) AS transaction_value
		FROM
			sales_2
		WHERE
			payment_method = 'WALLET_CASH'
				OR
			payment_method = 'CREDIT_LOAN'
		GROUP BY
			1,
			2,
			3,
			4
		ORDER BY
			1,
			2,
			3,
			4
	)
SELECT
	profile_id,
	cohort_month,
  country,
	transaction_month,
	transaction_count,
	transaction_value
FROM
	sales_3
UNION ALL
SELECT
	profile_id,
	cohort_month,
  country,
	transaction_month,
	transaction_count,
	transaction_value
FROM
	sales_4
ORDER BY
	1,
	2,
	3,
  4
  )
  group by 
  1,
  2,
  3
