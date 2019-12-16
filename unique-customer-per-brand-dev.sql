with 
overall_0 as (
  select 'ai' as brand, date_trunc(date(issued_date), month) as issued_date_utc7, contact_email, ROW_NUMBER()
				OVER(
					PARTITION BY
						contact_email
					ORDER BY
						issued_date
				)
				AS record_sequence from `tvlk-realtime.airy.fact_flight_sales`
  where issued_date is not null 
    
union all

  select 'pg' as brand, date_trunc(date(issued_date), month) as issued_date_utc7, contact_email, ROW_NUMBER()
				OVER(
					PARTITION BY
						contact_email
					ORDER BY
						issued_date
				)
				AS record_sequence from `tvlk-realtime.pegipegi.flight_sales`
  where issued_date is not null 
    
union all

  select 'tv' as brand, date_trunc(date(parse_datetime('%Y-%m-%d %H:%M:%E*S',issued_time)), month) as issued_date_utc7, contact_email, ROW_NUMBER()
				OVER(
					PARTITION BY
						contact_email
					ORDER BY
						date(parse_datetime('%Y-%m-%d %H:%M:%E*S',issued_time))
				)
				AS record_sequence from `tvlk-data-multibrand-prod.multibrand_flight.edw_fact_flight_booking_hashed` a
        join `tvlk-data-multibrand-prod.multibrand_flight.DIM_AIRPORT` b on b.string_field_0 = a.source_airport_id
  where issued_time is not null and b.string_field_2 = 'ID' and b.string_field_2 = 'ID' 
)
-- ,
-- overall_1 as (
-- -- check whether multi-brand or not.
-- select contact_email, count(distinct brand) as unique_brand  from overall_0
-- group by contact_email
-- order by count(distinct brand) desc
-- )

select brand, issued_date_utc7, count( distinct contact_email) from overall_0
group by brand, issued_date_utc7


-- -- check which brand is being used.
-- select distinct brand from overall_0
-- where contact_email='XhUqVTDk/0zglyDWp4aqAx0pAPxZ3cUQT29DLZtcxtJQoifNit5OiF3ua9bnzWHr+jby6I5XLqEpmcWf43LrMQ=='
