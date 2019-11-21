select 
DATE( TIMESTAMP_TRUNC( TIMESTAMP_ADD( TIMESTAMP_MILLIS( last_payment_assignment_timestamp ), INTERVAL 7 HOUR ), MONTH ) ) AS transaction_month,
SUBSTR(locale,-2) as country,
count(distinct(profile_id)) as unique_trx_user
from `tvlk-data-mkt-prod.datamart.sales_table`
where profile_id IS NOT NULL AND booking_status = 'ISSUED' 
group by 1,2
