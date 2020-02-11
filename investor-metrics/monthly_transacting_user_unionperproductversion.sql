with 
distinct_flight as
(
  select 'flight' as product_type, DATE( TIMESTAMP_TRUNC( TIMESTAMP_ADD( TIMESTAMP_MILLIS( issue_time ), INTERVAL 7 HOUR ), MONTH ) ) AS issued_month, country, profile_id, booking_id, num_seats as volume
  from (
    select
    *,
    row_number() over(partition by booking_id order by kafka_publish_timestamp desc) as rn
    from `tvlk-realtime.nrtprod.flight_booking` 
  )
  where rn = 1 and booking_Status='ISSUED'
),
distinct_hotel as
(
  select 'hotel' as product_type, DATE( DATETIME_TRUNC( DATETIME_ADD( kafka_publish_timestamp, INTERVAL 7 HOUR ), MONTH ) ) AS issued_month, country, profile_id, booking_id, num_of_nights as volume
  from (
    select
    *,
    row_number() over(partition by booking_id order by kafka_publish_timestamp desc) as rn
    from `tvlk-realtime.nrtprod.hotel_booking` 
  )
  where rn = 1 
  --and booking_Status='ISSUED'
),
  distinct_train as
(
  select 'train' as product_type, DATE( DATETIME_TRUNC( DATETIME_ADD( kafka_publish_timestamp, INTERVAL 7 HOUR ), MONTH ) ) AS issued_month, country, profile_id, bookingid as booking_id, num_adult as volume
  from (
    select
    *,
    row_number() over(partition by bookingid order by kafka_publish_timestamp desc) as rn
    from `tvlk-realtime.nrtprod.train_booking`
  )
  where rn = 1 
  --and booking_Status='ISSUED'
),

mixed as (

select issued_month, country, product_type, profile_id, booking_id, volume from distinct_flight
union all
select issued_month, country, product_type, profile_id, booking_id, volume from distinct_hotel
union all
select issued_month, country, product_type, profile_id, booking_id, volume from distinct_train
)

select issued_month, product_type, country, volume, count(distinct profile_id) as unique_trx_user from mixed
group by 1,2,3,4
