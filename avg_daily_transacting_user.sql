with
trx_day as (
select 
DATE( TIMESTAMP_TRUNC( TIMESTAMP_ADD( TIMESTAMP_MILLIS( last_payment_assignment_timestamp ), INTERVAL 7 HOUR ), DAY ) ) AS transaction_day,
SUBSTR(locale,-2) as country,
count(distinct(profile_id)) as unique_trx_user
from `tvlk-data-mkt-prod.datamart.sales_table`
where profile_id IS NOT NULL AND booking_status = 'ISSUED' 
group by 1,2
)
select date_trunc(transaction_day, month) as transaction_month, country, avg(unique_trx_user) as avg_unique_user from trx_day
group by 1,2
