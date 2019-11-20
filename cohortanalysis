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
			primary_sales_product_type AS product_type,
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
			CASE
				WHEN
					LSQ.product_type = 'DOMESTIC'
						OR
					LSQ.product_type = 'MOBILE_TOPUP'
						OR
					LSQ.product_type = 'MOBILE_DATA'
				THEN 'DOMESTIC_CONNECTIVITY'
				WHEN
					LSQ.product_type = 'EXPERIENCE'
						OR
					LSQ.product_type = 'CULINARY'
				THEN 'EXPERIENCE'
				WHEN
					LSQ.product_type = 'FLIGHT'
						OR
					LSQ.product_type = 'FLIGHT_CHECKIN'
				THEN 'FLIGHT'
				WHEN
					LSQ.product_type = 'WIFI_RENTAL'
						OR
					LSQ.product_type = 'PREPAID_SIM'
						OR
					LSQ.product_type = 'ROAMING'
				THEN 'INTERNATIONAL_CONNECTIVITY'
				WHEN
					LSQ.product_type = 'PLN_POSTPAID'
						OR
					LSQ.product_type = 'PLN_PREPAID'
						OR
					LSQ.product_type = 'BPJS_HEALTH'
						OR
					LSQ.product_type = 'PDAM_POSTPAID'
						OR
					LSQ.product_type = 'CABLE_SERVICE'
						OR
					LSQ.product_type = 'MOBILE_POSTPAID'
						OR
					LSQ.product_type = 'MULTI_FINANCE'
						OR
					LSQ.product_type = 'GAME_VOUCHER'
				THEN 'BILL_PAYMENT'
				ELSE LSQ.product_type
			END AS product_type,
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
			product_type,
			invoice_amount,
			payment_method,
      country,
			transaction_month
		FROM
			sales_1
		--WHERE
			--transaction_month <= DATE( '2019-05-31' )
	),
	sales_3 AS (
		SELECT
			product_type,
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
			4,
      5
		ORDER BY
			1,
			2,
			3,
			4,
      5
	),
	sales_4 AS (
		SELECT
			payment_method AS product_type,
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
			4,
      5
		ORDER BY
			1,
			2,
			3,
			4,
      5
	)
SELECT
	product_type,
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
	product_type,
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
  4,
  5
