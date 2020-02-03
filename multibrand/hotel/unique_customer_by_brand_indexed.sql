WITH
  base AS (
  SELECT
    TIMESTAMP_TRUNC(TIMESTAMP_ADD(s.issued_date, INTERVAL 7 HOUR), MONTH) AS month,
    s.brand,
    
    COUNT(DISTINCT s.contact_email) AS num_email,
    COUNT(DISTINCT
    case when abs(s.coupon_amount) > 0
    then
    s.contact_email end) AS num_email_coupon,
    COUNT(DISTINCT s.booking_id) AS num_issued,
    COUNT(DISTINCT
    case when abs(s.coupon_amount) > 0
    then
    s.booking_id end) AS num_issued_coupon,    
    sum(s.num_room_night) AS num_room_night,
    sum(abs(s.coupon_amount)) as coupon_cost,
    sum(s.total_provider_fare) as gbv,
    sum(abs(s.coupon_amount)) / sum(s.total_provider_fare) as coupon_gbv_prop,
    
    avg(s.total_provider_fare / s.num_room_night) as avg_adr,
    COUNT(DISTINCT
      case 
      when 
        cm_tv.first_booking_id_traveloka is not null
       or
         cm_ai.first_booking_id_airy is not null
       or
         cm_pg.first_booking_id_pegipegi is not null
       then s.contact_email end
    ) AS num_new_email,

    COUNT(DISTINCT case when travel_type = 'DOMESTIC' then s.booking_id end) AS num_issued_domestic,
    COUNT(DISTINCT case when travel_type = 'INTERNATIONAL' then s.booking_id end) AS num_issued_international,
    SUM(case when travel_type = 'DOMESTIC' then s.num_room_night end) AS num_room_night_domestic,
    SUM(case when travel_type = 'INTERNATIONAL' then s.num_room_night end) AS num_room_night_international,
    
    AVG(case when travel_type = 'DOMESTIC' then s.total_provider_fare end / case when travel_type = 'DOMESTIC' then s.num_room_night end) AS adr_domestic,
    AVG(case when travel_type = 'INTERNATIONAL' then s.total_provider_fare end / case when travel_type = 'INTERNATIONAL' then s.num_room_night end) AS adr_international
    
  FROM
    `tvlk-data-accom-dev.accom_multibrand.fact_accommodation_sales` s
    
  left join `tvlk-data-accom-dev.accom_multibrand.accom_multibrand_customer_master` cm_tv
    on s.booking_id = cm_tv.first_booking_id_traveloka
    and s.brand = 'TRAVELOKA'

  left join `tvlk-data-accom-dev.accom_multibrand.accom_multibrand_customer_master` cm_ai
    on s.booking_id = cm_ai.first_booking_id_airy
    and s.brand = 'AIRY'

  left join `tvlk-data-accom-dev.accom_multibrand.accom_multibrand_customer_master` cm_pg
    on s.booking_id = cm_pg.first_booking_id_pegipegi
    and s.brand = 'PEGIPEGI'   
    
    
  WHERE
    s.issued_date >= TIMESTAMP('2017-11-30 17:00:00')
    and s.issued_date < TIMESTAMP('2018-12-31 17:00:00')
    and case when s.brand = 'AIRY' and s.affiliate_id in ('traveloka', 'pegipegi') then false else true end
    and s.country_id = 'ID'
  GROUP BY
    1,
    2 )
SELECT
  month,
  brand,
  num_email,
  num_email / FIRST_VALUE(num_email) OVER (PARTITION BY brand ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS indexed_num_email,

  num_email_coupon,
  num_email_coupon / FIRST_VALUE(num_email_coupon) OVER (PARTITION BY brand ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS indexed_num_email_coupon,


  num_issued,
  num_issued / FIRST_VALUE(num_issued) OVER (PARTITION BY brand ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS indexed_num_issued,

  num_issued_coupon,
  num_issued_coupon / FIRST_VALUE(num_issued_coupon) OVER (PARTITION BY brand ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS indexed_num_issued_coupon,

  num_room_night,
  num_room_night / FIRST_VALUE(num_room_night) OVER (PARTITION BY brand ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS indexed_num_room_night,
  
  coupon_cost,
  coupon_cost / FIRST_VALUE(coupon_cost) OVER (PARTITION BY brand ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS indexed_coupon_cost,

  gbv,
  gbv / FIRST_VALUE(gbv) OVER (PARTITION BY brand ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS indexed_gbv,

  coupon_gbv_prop,
  coupon_gbv_prop / FIRST_VALUE(coupon_gbv_prop) OVER (PARTITION BY brand ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS indexed_coupon_gbv_prop,


  avg_adr,
  avg_adr / FIRST_VALUE(avg_adr) OVER (PARTITION BY brand ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS indexed_avg_adr,

  num_new_email,
  num_new_email / FIRST_VALUE(num_new_email) OVER (PARTITION BY brand ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS indexed_num_new_email,
  
  num_issued_domestic,
  num_issued_domestic / FIRST_VALUE(num_issued_domestic) OVER (PARTITION BY brand ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS indexed_num_issued_domestic,
  
  num_issued_international,
  num_issued_international / NULLIF(FIRST_VALUE(num_issued_international) OVER (PARTITION BY brand ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS indexed_num_issued_international,
  
  num_room_night_domestic,
  num_room_night_domestic / FIRST_VALUE(num_room_night_domestic) OVER (PARTITION BY brand ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS indexed_num_room_night_domestic,
  
  num_room_night_international,
  num_room_night_international / NULLIF(FIRST_VALUE(num_room_night_international) OVER (PARTITION BY brand ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS indexed_num_room_night_international,
  
  adr_domestic,
  adr_domestic / FIRST_VALUE(adr_domestic) OVER (PARTITION BY brand ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS indexed_adr_domestic,
  
  adr_international,
  adr_international / NULLIF(FIRST_VALUE(adr_international) OVER (PARTITION BY brand ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS indexed_adr_international
  
FROM
  base
