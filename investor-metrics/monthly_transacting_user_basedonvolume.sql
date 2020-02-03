with 
  distinct_flight as 
(
  select 'flight' as product_type, DATE( TIMESTAMP_TRUNC( TIMESTAMP_ADD( TIMESTAMP_MILLIS( issue_time ), INTERVAL 7 HOUR ), MONTH ) ) AS issued_month, country, profile_id, booking_id, num_seats as volume from `tvlk-realtime.nrtprod.flight_booking` 
  where booking_Status='ISSUED'
),
  distinct_hotel as
(
  select 'hotel' as product_type, DATE( DATETIME_TRUNC( DATETIME_ADD( kafka_publish_timestamp, INTERVAL 7 HOUR ), MONTH ) ) AS issued_month, SUBSTR(locale,-2) as country, profile_id, booking_id, num_of_nights as volume from `tvlk-realtime.nrtprod.hotel_booking` 
  group by 1,2,3,4,5,6
),
  distinct_train as
(
  select 'train' as product_type, DATE( DATETIME_TRUNC( DATETIME_ADD( kafka_publish_timestamp, INTERVAL 7 HOUR ), MONTH ) ) AS issued_month, country, profile_id, bookingid as booking_id, num_adult as volume from `tvlk-realtime.nrtprod.train_booking`
),

mixed as 
(
select
  DATE( TIMESTAMP_TRUNC( TIMESTAMP_ADD( TIMESTAMP_MILLIS( last_payment_assignment_timestamp ), INTERVAL 7 HOUR ), MONTH ) ) AS transaction_month,
  SUBSTR(a.locale,-2) as country,
  a.product,
  case 
    when a.product='FLIGHT' then flight.volume
    when a.product='HOTEL' then hotel.volume
    when a.product='TRAIN' then train.volume
  end as volume,
  a.profile_id
from `tvlk-data-mkt-prod.datamart.sales_table` a
left join distinct_flight flight on flight.booking_id = a.booking_id
left join distinct_hotel hotel on hotel.booking_id = a.booking_id
left join distinct_train train on train.booking_id = a.booking_id
where a.profile_id IS NOT NULL 
and a.booking_status = 'ISSUED' 
and product in ('FLIGHT','TRAIN','HOTEL')
)

select transaction_month, country, product, volume, count(distinct profile_id) as unique_trx_user from mixed
group by 1,2,3,4
order by 1
