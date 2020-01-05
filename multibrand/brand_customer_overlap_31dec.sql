with

rank_TV as
(
  select *,
  ROW_NUMBER()
          OVER(
            PARTITION BY
              booking_id
            ORDER BY
              PARTITIONTIME
          )
          AS record_seq
  from `tvlk-data-multibrand-prod.multibrand_flight.edw_fact_flight_booking_hashed`
),

distinct_TV_0 as
(
  select * from rank_TV
  where record_seq=1
),

rank_AI as
(
  SELECT rank() over (partition by booking_id order by data_id) as row_no, * FROM `tvlk-realtime.airy.fact_flight_sales`
),

distinct_AI_0 as
(
  select * from rank_AI
  where row_no = 1
),

rank_PG as
(
  SELECT rank() over (partition by booking_id order by data_id) as row_no, * FROM `tvlk-realtime.pegipegi.flight_sales`
),

distinct_PG_0 as
(
  select * from rank_PG
  where row_no = 1
),

conversion_0 AS (
		SELECT
			a.conversion_table_id AS conversion_id,
      date_trunc(date(timestamp_millis(approved_timestamp)), month) as approved_timestamp,
			b.source_currency,
			b.target
		FROM
			`tvlk-data-mkt-prod`.datamart.conversion_table AS a
				CROSS JOIN
			UNNEST( conversion_data ) AS b
	),
	conversion_1 AS (
			SELECT
				a.conversion_id,
        approved_timestamp,
				a.source_currency,
				b.currency AS target_currency,
				b.exchange_rates
			FROM
				conversion_0 AS a
					CROSS JOIN
				UNNEST( target ) AS b
			WHERE
				b.currency = 'IDR'
	),

trunc_0 as
(
select *, date_trunc(date(parse_datetime('%Y-%m-%d %H:%M:%E*S',issued_time)), month) as month from  distinct_TV_0
),

distinct_tv as
(
  select 
    contact_email as tv_contact_email, 
    sum(a.total_fare * IF(e.exchange_rates IS NULL AND a.currency_id = 'IDR',1,e.exchange_rates)) as total_fare, 
    count(distinct booking_id) as num_booking 
  from trunc_0 a
  left join `tvlk-data-multibrand-prod.multibrand_flight.DIM_AIRPORT` b on b.string_field_0 = a.source_airport_id
  left join `tvlk-data-multibrand-prod.multibrand_flight.DIM_AIRPORT` d on d.string_field_0 = a.destination_airport_id
  left join conversion_1 e on e.source_currency = a.currency_id and e.approved_timestamp = a.month
  where issued_time is not null and b.string_field_2 = 'ID' and d.string_field_2 = 'ID'
  and date(parse_datetime('%Y-%m-%d %H:%M:%E*S',issued_time))<='2019-12-31'
  group by contact_email
),
distinct_pg as
(
  select 
    contact_email as pg_contact_email, 
    sum(total_fare) as total_fare, 
    count(booking_id) as num_booking  
  from distinct_PG_0
  where issued_date is not null
  and date(issued_date)<='2019-12-31'
  group by contact_email
),
distinct_ai as
(
  select 
    TO_BASE64(FROM_HEX(contact_email)) as ai_contact_email, 
    sum(total_fare) as total_fare, 
    count(booking_id) as num_booking
  from distinct_AI_0
  where issued_date is not null
  and date(issued_date)<='2019-12-31'
  group by contact_email
),

overlap_0 as
(
  select 
    tv_contact_email, 
    ai_contact_email, 
    case
      when tv_contact_email is null then 'AI only'
      when ai_contact_email is null then 'TV only'
      else 'TV and AI'
    end as customer_overlap,
    case
      when tv_contact_email is null then b.total_fare
      when ai_contact_email is null then a.total_fare
      else (a.total_fare + b.total_fare)
    end as total_fare,
    case
      when tv_contact_email is null then b.num_booking
      when ai_contact_email is null then a.num_booking
      else (a.num_booking + b.num_booking)
    end as num_booking,
    coalesce(tv_contact_email, ai_contact_email) as mixed_contact_email
  from distinct_tv a
  full outer join distinct_ai b on b.ai_contact_email = a.tv_contact_email
),

left_0 as
(
  select 
    case when b.pg_contact_email is not null then (a.total_fare + b.total_fare) else a.total_fare end as total_fare,
    case when b.pg_contact_email is not null then (a.num_booking + b.num_booking) else a.num_booking end as num_booking,
    case when b.pg_contact_email is not null then 'TV and PG' else 'TV only' end as user_segment  
  from overlap_0 a
  left join distinct_pg b on b.pg_contact_email = a.mixed_contact_email
  where a.customer_overlap='TV only'

  union all

  select 
    case when b.pg_contact_email is not null then (a.total_fare + b.total_fare) else a.total_fare end as total_fare,
    case when b.pg_contact_email is not null then (a.num_booking + b.num_booking) else a.num_booking end as num_booking,
    case when b.pg_contact_email is not null then 'TV, AI, and PG' else 'TV and AI' end as user_segment
  from overlap_0 a
  left join distinct_pg b on b.pg_contact_email = a.mixed_contact_email
  where a.customer_overlap='TV and AI'

  union all

  select 
    case when b.pg_contact_email is not null then (a.total_fare + b.total_fare) else a.total_fare end as total_fare,
    case when b.pg_contact_email is not null then (a.num_booking + b.num_booking) else a.num_booking end as num_booking,
    case when b.pg_contact_email is not null then 'AI and PG' else 'AI only' end as user_segment
  from overlap_0 a
  left join distinct_pg b on b.pg_contact_email = a.mixed_contact_email
  where a.customer_overlap='AI only'
),

left_1 as
(
  select tv_contact_email as contact_email_final, total_fare, num_booking 
  from distinct_tv

  union all

  select pg_contact_email as contact_email_final, total_fare, num_booking 
  from distinct_pg

  union all

  select ai_contact_email as contact_email_final, total_fare, num_booking 
  from distinct_ai
),

left_2 as
(
  select contact_email_final, sum(total_fare) as total_fare_final, sum(num_booking) as num_booking_final from left_1
  group by contact_email_final
),

final_0 as
(
  select user_segment, count(user_segment) as num_customer, sum(total_fare) as total_fare, sum(num_booking) as num_booking 
  from left_0
  group by user_segment
)

select 
  date_sub(date_trunc(current_date,month), interval 1 day) as snapshot_month, 
  'PG only' as user_segment, 
  count(contact_email_final) - (select sum(num_customer) from final_0) as num_customer
  ,sum(total_fare_final) - (select sum(total_fare) from final_0) as total_fare
  ,sum(num_booking_final) - (select sum(num_booking) from final_0) as num_booking
from left_2

union all

select 
  date_sub(date_trunc(current_date,month), interval 1 day) as snapshot_month,
  user_segment, 
  num_customer, 
  total_fare, 
  num_booking 
from final_0
order by user_segment




