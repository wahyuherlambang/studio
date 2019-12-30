with
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
trunc_0 as (
select *, date_trunc(date(parse_datetime('%Y-%m-%d %H:%M:%E*S',issued_time)), month) as month from  `tvlk-data-multibrand-prod.multibrand_flight.edw_fact_flight_booking_hashed`
),

overall_0 as (
  select 'AI' as brand, issued_date, 
  TO_BASE64(FROM_HEX(contact_email)) as contact_email,
  count(booking_id) as num_booking,
  sum(total_fare) as total_fare,
  sum(coupon_amount) as coupon_amount
  from `tvlk-realtime.airy.fact_flight_sales` a
  where issued_date is not null
  group by 1,2,3
  
union all

  select 'PG' as brand, issued_date, 
  contact_email,
  count(booking_id) as num_booking,
  sum(total_fare) as total_fare,
  sum(coupon_amount) as coupon_amount
  from `tvlk-realtime.pegipegi.flight_sales`
  where issued_date is not null
  group by 1,2,3
  
union all

  select 'TV' as brand, parse_datetime('%Y-%m-%d %H:%M:%E*S',issued_time) as issued_date, 
  contact_email,
  count(cast(a.booking_id as string)) as num_booking,
  sum(a.total_fare * IF(e.exchange_rates IS NULL AND a.currency_id = 'IDR',1,e.exchange_rates)) as total_fare,
  sum(c.coupon_amount) as coupon_amount
  from trunc_0 a
  join `tvlk-data-multibrand-prod.multibrand_flight.DIM_AIRPORT` b on b.string_field_0 = a.source_airport_id
  join `tvlk-data-multibrand-prod.multibrand_flight.DIM_AIRPORT` d on d.string_field_0 = a.source_airport_id
  left join `tvlk-data-corporate-dev.multibrand.tv_coupon_amount_prod` c on c.booking_id = a.booking_id
  left join conversion_1 e on e.source_currency = a.currency_id and e.approved_timestamp = a.month
  where issued_time is not null and b.string_field_2 = 'ID' and d.string_field_2 = 'ID'
  group by 1,2,3
),

df_transaction as 
(
  select issued_date, contact_email, num_booking, total_fare, coupon_amount, total_fare - coupon_amount as GBV_minus_coupon,
    ROW_NUMBER()
          OVER(
            PARTITION BY
              contact_email
            ORDER BY
              issued_date
              )
            AS record_seq
   from overall_0
),

first_user as
(
  select brand, a.issued_date, a.contact_email, a.num_booking, a.total_fare, a.coupon_amount, a.GBV_minus_coupon, record_seq from df_transaction a
  left join overall_0 b on b.contact_email = a.contact_email and b.issued_date = a.issued_date
  where record_seq=1
),

first_user_distinct as
(
  select distinct * from first_user
),


existing_user as
(
  select brand, a.issued_date, a.contact_email, a.num_booking, a.total_fare, a.coupon_amount, a.GBV_minus_coupon, record_seq from df_transaction a
  left join overall_0 b on b.contact_email = a.contact_email and b.issued_date = a.issued_date
  where record_seq>1
),

existing_user_distinct as
(
select distinct * from existing_user
),

cross_sell as 
(
  select 
    case 
      when b.brand = 'AI' then 'First AI'
      when b.brand = 'PG' then 'First PG'
      when b.brand = 'TV' then 'First TV'
    end as first_brand,
    ifnull(a.brand, 'first purchase') as existing_brand,
    count( distinct a.contact_email) as unique_customer_existing,
    count(distinct b.contact_email) as unique_customer_first,
    sum(a.GBV_minus_coupon)/count(a.num_booking) as GBV_minus_coupon_per_trx_existing,
    sum(b.GBV_minus_coupon)/count(b.num_booking) as GBV_minus_coupon_per_trx_first
  from existing_user_distinct a
  full outer join first_user_distinct b on b.contact_email = a.contact_email
  group by 1,2
)

select 
  first_brand, 
  existing_brand, 
  case 
    when unique_customer_existing = 0 then unique_customer_first
    else unique_customer_existing
  end as unique_customer,
    case 
    when GBV_minus_coupon_per_trx_existing = 0 then GBV_minus_coupon_per_trx_first
    else GBV_minus_coupon_per_trx_existing
  end as GBV_minus_coupon_per_trx
from cross_sell
order by first_brand

