with
-- making source table for currencty conversion
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
  
-------make distinct tvlk data
-- adding conversion_rate_id into tvlk data
rate_0 as
(
  select booking_id, conversion_rate_id, currency_id, _PARTITIONTIME from `tvlk-data-flight-prod.flight_multibrand.edw_fact_flight_booking`
  union all
  select booking_id, conversion_rate_id, currency_id, _PARTITIONTIME from `tvlk-data-flight-prod.flight_multibrand.edw_fact_flight_booking_old`
),

-- adding row_number, partition by booking id based on partition time on rate_0
dedup_rate_0 as
(
  select *,
  ROW_NUMBER()
          OVER(
            PARTITION BY
              booking_id
            ORDER BY
              _PARTITIONTIME
          )
          AS record_seq
  from rate_0
),

-- deduplicate rate_0
dedup_rate_1 as
(
  select * from dedup_rate_0
  where record_seq=1
),

--adding row number, partition by booking id based on partition time on edw fact flight booking hashed.
dedup_fact_0 as
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

-- deduplicate fact_0
dedup_fact_1 as
(
  select * from dedup_fact_0
  where record_seq=1
),

--adding date trunc on dedup_fact_1
trunc_0 as
(
  select *, date_trunc(date(parse_datetime('%Y-%m-%d %H:%M:%E*S',issued_time)), month) as month, parse_datetime('%Y-%m-%d %H:%M:%E*S',issued_time) as issued_datetime  from  dedup_fact_1
),

-- join between trunc_0 and dedup_rate_1 (contain conversion_rate_id)
distinct_tv as
(
  select a.*, coupon_amount, rate.conversion_rate_id, a.total_fare * IF(e.exchange_rates IS NULL AND a.currency_id = 'IDR',1,e.exchange_rates) as net_total_fare, case when (coupon_amount is null or coupon_amount=0) then 0 else 1 end used_coupon from trunc_0 a
  left join dedup_rate_1 as rate on rate.booking_id = a.booking_id
  join `tvlk-data-multibrand-prod.multibrand_flight.DIM_AIRPORT` b on b.string_field_0 = a.source_airport_id
  join `tvlk-data-multibrand-prod.multibrand_flight.DIM_AIRPORT` d on d.string_field_0 = a.source_airport_id
  left join `tvlk-data-corporate-dev.multibrand.tv_coupon_amount_prod` c on c.booking_id = a.booking_id
  left join conversion_1 e on e.source_currency = rate.currency_id and e.conversion_id = rate.conversion_rate_id
  where issued_time is not null and b.string_field_2 = 'ID' and d.string_field_2 = 'ID'
  and date(parse_datetime('%Y-%m-%d %H:%M:%E*S',issued_time)) >= '2018-12-01'
  and date(parse_datetime('%Y-%m-%d %H:%M:%E*S',issued_time)) <= '2019-12-31'
   
),
------- end of making distinct tvlk data

-- adding row number, partition by booking id based on data_id on airy fact flight sales
rank_AI as
(
  SELECT rank() over (partition by booking_id order by data_id) as row_no, * FROM `tvlk-realtime.airy.fact_flight_sales`
),

-- deduplicate rank_AI
distinct_AI as
(
  select *, case when (coupon_amount is null or coupon_amount=0) then 0 else 1 end used_coupon from rank_AI
  where row_no = 1 and issued_date >= '2018-12-01' and issued_date <= '2019-12-31'

),

--adding row number, partition by booking id based on data_id on pegipegi flight sales
rank_PG as
(
  SELECT rank() over (partition by booking_id order by data_id) as row_no, * FROM `tvlk-realtime.pegipegi.flight_sales`
),

--deduplicate rank_pg
distinct_PG as
(
  select *, case when (coupon_amount is null or coupon_amount=0) then 0 else 1 end used_coupon from rank_PG
  where row_no = 1 
  and issued_date >= '2018-12-01' and issued_date <= '2019-12-31'
),

raw_1 as
(
select 'AI' as brand, issued_date, TO_BASE64(FROM_HEX(contact_email)) as contact_email, booking_id, coupon_amount, used_coupon, total_fare from distinct_AI
where date(issued_date) <= '2019-12-31'
union all
select 'PG' as brand, issued_date, contact_email, booking_id, coupon_amount, used_coupon, total_fare from distinct_PG
where date(issued_date) <= '2019-12-31'
union all
select 'TV' as brand, parse_datetime('%Y-%m-%d %H:%M:%E*S',issued_time) as issued_date, contact_email, cast(booking_id as string) as booking_id, coupon_amount, used_coupon, total_fare from distinct_TV
where date(parse_datetime('%Y-%m-%d %H:%M:%E*S',issued_time)) <= '2019-12-31'
),

first_brand as 
(
select 
  issued_date,
  brand,
  contact_email, 
  booking_id, 
  coupon_amount,
  used_coupon,
  total_fare,
  FIRST_VALUE(brand)
      OVER (PARTITION BY contact_email ORDER BY issued_date ASC) 
  as first_brand
from raw_1
group by 1,2,3,4,5,6,7, issued_date
),

temp_0 as
(
  select contact_email, count(booking_id) as num_booking from raw_1
  group by 1
),

temp_1 as
(
  select * from temp_0
  where num_booking = 1
),

first_existing as
(
select a.*, total_fare - coupon_amount as gbv_coupon, case when b.contact_email is not null then 'first_purchaser' else a.brand end as existing_brand from first_brand a
left join temp_1 as b on b.contact_email = a.contact_email
),

cannibal_1 as
(
select 
  first_brand,
  existing_brand,
  contact_email, 
  count(booking_id) as num_booking, 
  sum(coupon_amount) as coupon_amount, 
  sum(used_coupon) as used_coupon,
  case when sum(used_coupon) / count(booking_id) >0.5 then 1 else 0 end as is_coupon_hunter,
  sum(total_fare) as total_fare,
  sum(gbv_coupon) as gbv_coupon_1,
  --sum(total_fare - coupon_amount) as gbv_coupon_2
  sum(gbv_coupon)/count(booking_id) as gbv_coupon_pertrx
  
from first_existing
--where first_brand = 'AI' and existing_brand = 'PG'
group by 1,2,3
),

cannibal_2 as
(
select 
  first_brand, 
  existing_brand, 
  count(distinct contact_email) as unique_customer, 
  sum(num_booking) as num_booking,
  sum(is_coupon_hunter) as num_coupon_hunter,
  sum((total_fare - coupon_amount))/sum(num_booking) as gbv_coupon_pertrx
  from cannibal_1
  group by 1,2
)

select * from cannibal_2
