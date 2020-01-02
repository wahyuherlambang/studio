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

distinct_TV as
(
  select * from rank_TV
  where record_seq=1
),

rank_AI as
(
  SELECT rank() over (partition by booking_id order by data_id) as row_no, * FROM `tvlk-realtime.airy.fact_flight_sales`
),

distinct_AI as
(
  select * from rank_AI
  where row_no = 1
),

rank_PG as
(
  SELECT rank() over (partition by booking_id order by data_id) as row_no, * FROM `tvlk-realtime.pegipegi.flight_sales`
),

distinct_PG as
(
  select * from rank_PG
  where row_no = 1
),

overall_0 as (
  select 'AI' as brand, issued_date, 
  TO_BASE64(FROM_HEX(contact_email)) as contact_email,
  booking_id
  from distinct_AI a
  where issued_date is not null
  
union all

  select 'PG' as brand, issued_date, 
  contact_email,
  booking_id
  from distinct_PG
  where issued_date is not null
  
union all

  select 'TV' as brand, parse_datetime('%Y-%m-%d %H:%M:%E*S',issued_time) as issued_date, 
  contact_email,
  cast(a.booking_id as string) as booking_id,
  from distinct_TV a
  join `tvlk-data-multibrand-prod.multibrand_flight.DIM_AIRPORT` b on b.string_field_0 = a.source_airport_id
  join `tvlk-data-multibrand-prod.multibrand_flight.DIM_AIRPORT` d on d.string_field_0 = a.source_airport_id
  where issued_time is not null and b.string_field_2 = 'ID' and d.string_field_2 = 'ID'
),

df_transaction as 
(
  select issued_date, contact_email, 
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
  select brand, a.issued_date, a.contact_email, record_seq from df_transaction a
  left join overall_0 b on b.contact_email = a.contact_email and b.issued_date = a.issued_date
  where record_seq=1
),

first_user_distinct as
(
  select distinct * from first_user
),


existing_user as
(
  select brand, a.issued_date, a.contact_email, record_seq from df_transaction a
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
    count(distinct b.contact_email) as unique_customer_first
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
  end as unique_customer        
from cross_sell
order by first_brand
