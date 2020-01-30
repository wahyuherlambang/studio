with a as (
select
DATE( TIMESTAMP_TRUNC( TIMESTAMP_ADD( TIMESTAMP_MILLIS( last_payment_assignment_timestamp ), INTERVAL 7 HOUR ), MONTH ) ) AS transaction_month,
SUBSTR(locale,-2) as country,
product,
case 
  when product='FLIGHT' then flight.total_seat
  when product='HOTEL' then hotel.room_night
  when product='TRAIN' then train.num_adult
end as volume,
a.profile_id
from `tvlk-data-mkt-prod.datamart.sales_table` a
left join `tvlk-realtime.nrtprod.flight_issued` flight on flight.booking_id = a.booking_id
left join `tvlk-realtime.nrtprod.hotel_issued` hotel on hotel.booking_id = a.booking_id
left join `tvlk-realtime.nrtprod.train_booking` train on train.bookingid = a.booking_id
where a.profile_id IS NOT NULL AND booking_status = 'ISSUED' and product in ('FLIGHT','TRAIN','HOTEL')
)

select transaction_month, country, product, volume, count(distinct profile_id) as unique_trx_user from a
group by 1,2,3,4
