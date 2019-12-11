with 
brand as (
  select 'ai' as brand, date_trunc(date(issued_date), month) as issued_date_utc7, count(distinct(contact_email)) as unique_cust from `tvlk-realtime.airy.fact_flight_sales`
  where issued_date is not null 
    --and booking_status = 'ISSUED'
    and issued_date >= '2017-12-01'
    and issued_date <= '2018-12-31'
  group by date_trunc(date(issued_date), month)
union all

  select 'pg' as brand, date_trunc(date(issued_date), month) as issued_date_utc7, count(distinct(contact_email)) as unique_cust from `tvlk-realtime.pegipegi.flight_sales`
  where issued_date is not null 
    --and booking_status = 'ISSUED'
    and issued_date >= '2017-12-01'
    and issued_date <= '2018-12-31'
  group by date_trunc(date(issued_date), month)
union all

  select 'tv' as brand, date_trunc(date(parse_datetime('%Y-%m-%d %H:%M:%E*S',issued_time)), month) as issued_date_utc7,count(distinct(contact_email)) as unique_cust from `tvlk-data-multibrand-prod.multibrand_flight.edw_fact_flight_booking_hashed`
  where issued_time is not null 
    --and booking_status = 'ISSUED'
    and issued_time >= '2017-12-01'
    and issued_time <= '2018-12-31'
  group by date_trunc(date(parse_datetime('%Y-%m-%d %H:%M:%E*S',issued_time)), month)
)
select * from brand
       
