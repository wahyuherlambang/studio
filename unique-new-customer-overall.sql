with 
overall_0 as (
  select date_trunc(date(issued_date), month) as issued_date_utc7, contact_email, ROW_NUMBER()
				OVER(
					PARTITION BY
						contact_email
					ORDER BY
						issued_date
				)
				AS record_sequence from `tvlk-realtime.airy.fact_flight_sales`
  where issued_date is not null 
    --and booking_status = 'ISSUED'
    and issued_date >= '2017-12-01'
    and issued_date <= '2018-12-31'
    
union all

  select date_trunc(date(issued_date), month) as issued_date_utc7, contact_email, ROW_NUMBER()
				OVER(
					PARTITION BY
						contact_email
					ORDER BY
						issued_date
				)
				AS record_sequence from `tvlk-realtime.pegipegi.flight_sales`
  where issued_date is not null 
    --and booking_status = 'ISSUED'
    and issued_date >= '2017-12-01'
    and issued_date <= '2018-12-31'
    
union all

  select date_trunc(date(parse_datetime('%Y-%m-%d %H:%M:%E*S',issued_time)), month) as issued_date_utc7, contact_email, ROW_NUMBER()
				OVER(
					PARTITION BY
						contact_email
					ORDER BY
						date(parse_datetime('%Y-%m-%d %H:%M:%E*S',issued_time))
				)
				AS record_sequence from `tvlk-data-multibrand-prod.multibrand_flight.edw_fact_flight_booking_hashed`
  where issued_time is not null 
    --and booking_status = 'ISSUED'
    and issued_time >= '2017-12-01'
    and issued_time <= '2018-12-31'
),

overall_1 as (
select * from overall_0
where record_sequence=1
)


select issued_date_utc7, count(contact_email) as unique_new_customer from overall_1
group by issued_date_utc7
