with
  trx_0 as (
  select profile_id, product, DATE( TIMESTAMP_TRUNC( TIMESTAMP_ADD( TIMESTAMP_MILLIS( last_payment_assignment_timestamp ), INTERVAL 7 HOUR ), DAY ) ) AS transaction_day,
  ROW_NUMBER()
				OVER(
					PARTITION BY
						booking_id
					ORDER BY
						last_payment_assignment_timestamp
				)
				AS record_sequence
  from `tvlk-data-mkt-prod.datamart.sales_table`
  where profile_id IS NOT NULL AND booking_status = 'ISSUED'
  ),
  user_trx as (
  select profile_id, product, date_trunc(transaction_day,month) as transaction_month from trx_0
  where record_sequence=1
  ),
  first_trx as (
  select profile_id, product as first_product, 
         MIN (transaction_month)
				 OVER( PARTITION BY profile_id)
				 AS first_transaction_month from user_trx
  )
 select a.profile_id, a.product, b.first_product, a.transaction_month, b.first_transaction_month from user_trx a
 left join first_trx as b on b.profile_id = a.profile_id
