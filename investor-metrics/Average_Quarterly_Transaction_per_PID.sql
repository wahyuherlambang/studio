select
DATE( TIMESTAMP_TRUNC( TIMESTAMP_ADD( TIMESTAMP_MILLIS( last_payment_assignment_timestamp ), INTERVAL 7 HOUR ), QUARTER ) ) AS transaction_quarter,
SUBSTR(locale,-2) as country,
count(booking_id) as num_trx,
count(distinct(profile_id)) as unique_trx_user,
round(count(booking_id)/count(distinct(profile_id))) as avg_quarterly_trx_per_user
from `tvlk-data-mkt-prod.datamart.sales_table`
where profile_id IS NOT NULL AND booking_status = 'ISSUED' and last_payment_assignment_timestamp is not null
group by 1,2
