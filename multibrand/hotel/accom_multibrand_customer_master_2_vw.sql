with overall as (
SELECT distinct
  contact_email,
  FIRST_VALUE(brand IGNORE NULLS) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS first_brand,
  FIRST_VALUE(booking_id IGNORE NULLS) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS first_booking_id,
  FIRST_VALUE(issued_date IGNORE NULLS) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS first_issued_date,
  LAST_VALUE(booking_id IGNORE NULLS) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS last_booking_id,
  LAST_VALUE(issued_date IGNORE NULLS) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS last_issued_date,
  COUNT(booking_id) OVER(PARTITION BY contact_email) AS num_issued,
  COUNT(CASE
      WHEN coupon_amount < 0 THEN booking_id END) OVER(PARTITION BY contact_email) AS num_issued_coupon,
  SUM(total_provider_fare) OVER(PARTITION BY contact_email) AS total_gbv,
  AVG(total_provider_fare / num_room_night) OVER(PARTITION BY contact_email) AS avg_adr,
  SUM(total_purchase_fare) OVER(PARTITION BY contact_email) AS total_spent,
  SUM(total_commission) OVER(PARTITION BY contact_email) AS total_commission,
  SUM(total_premium) OVER(PARTITION BY contact_email) AS total_premium,
  SUM(coupon_amount) OVER(PARTITION BY contact_email) AS total_coupon_amount,
  COUNT(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN booking_id END) OVER(PARTITION BY contact_email) AS num_issued_2018,
  COUNT(CASE
      WHEN coupon_amount < 0 AND issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN booking_id END) OVER(PARTITION BY contact_email) AS num_issued_coupon_2018,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN total_provider_fare END) OVER(PARTITION BY contact_email) AS total_gbv_2018,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN total_purchase_fare END) OVER(PARTITION BY contact_email) AS total_spent_2018,
      SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN total_commission END) OVER(PARTITION BY contact_email) AS total_commission_2018,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN total_premium END) OVER(PARTITION BY contact_email) AS total_premium_2018,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN coupon_amount END) OVER(PARTITION BY contact_email) AS total_coupon_amount_2018,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN num_room_night END) OVER(PARTITION BY contact_email) AS num_room_night_2018,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN num_room END) OVER(PARTITION BY contact_email) AS num_room_2018,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN num_night END) OVER(PARTITION BY contact_email) AS num_night_2018,  
  PERCENTILE_CONT(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN DATE_DIFF(check_in_date, date(issued_date), DAY) END, 0.5) OVER(PARTITION BY contact_email) AS median_booking_windows_2018    
FROM
  `tvlk-data-accom-dev.accom_multibrand.fact_accommodation_sales`
WHERE
  case 
  when brand != 'AIRY' then true
  when brand = 'AIRY' and (affiliate_id not in ('traveloka', 'pegipegi') or affiliate_id is null) then true else false end
  and country_id = 'ID'
  and issued_date is not null
), traveloka as (
SELECT distinct
  contact_email as tvl_email,
  FIRST_VALUE(brand IGNORE NULLS) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS first_brand_traveloka,
  FIRST_VALUE(booking_id IGNORE NULLS) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS first_booking_id_traveloka,
  FIRST_VALUE(issued_date IGNORE NULLS) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS first_issued_date_traveloka,
  LAST_VALUE(booking_id IGNORE NULLS) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS last_booking_id_traveloka,
  LAST_VALUE(issued_date) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS last_issued_date_traveloka,
  COUNT(booking_id) OVER(PARTITION BY contact_email) AS num_issued_traveloka,
  COUNT(CASE
      WHEN coupon_amount < 0 THEN booking_id END) OVER(PARTITION BY contact_email) AS num_issued_coupon_traveloka,
  SUM(total_provider_fare) OVER(PARTITION BY contact_email) AS total_gbv_traveloka,
  AVG(total_provider_fare / num_room_night) OVER(PARTITION BY contact_email) AS avg_adr_traveloka,
  SUM(total_purchase_fare) OVER(PARTITION BY contact_email) AS total_spent_traveloka,
  SUM(total_commission) OVER(PARTITION BY contact_email) AS total_commission_traveloka,
  SUM(total_premium) OVER(PARTITION BY contact_email) AS total_premium_traveloka,
  SUM(coupon_amount) OVER(PARTITION BY contact_email) AS total_coupon_amount_traveloka,
  COUNT(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN booking_id END) OVER(PARTITION BY contact_email) AS num_issued_2018_traveloka,
  COUNT(CASE
      WHEN coupon_amount < 0 AND issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN booking_id END) OVER(PARTITION BY contact_email) AS num_issued_coupon_2018_traveloka,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN total_provider_fare END) OVER(PARTITION BY contact_email) AS total_gbv_2018_traveloka,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN total_purchase_fare END) OVER(PARTITION BY contact_email) AS total_spent_2018_traveloka,
      SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN total_commission END) OVER(PARTITION BY contact_email) AS total_commission_2018_traveloka,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN total_premium END) OVER(PARTITION BY contact_email) AS total_premium_2018_traveloka,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN coupon_amount END) OVER(PARTITION BY contact_email) AS total_coupon_amount_2018_traveloka,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN num_room_night END) OVER(PARTITION BY contact_email) AS num_room_night_2018_traveloka,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN num_room END) OVER(PARTITION BY contact_email) AS num_room_2018_traveloka,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN num_night END) OVER(PARTITION BY contact_email) AS num_night_2018_traveloka,
  PERCENTILE_CONT(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN DATE_DIFF(check_in_date, date(issued_date), DAY) END, 0.5) OVER(PARTITION BY contact_email) AS median_booking_windows_2018_traveloka  
FROM
  `tvlk-data-accom-dev.accom_multibrand.fact_accommodation_sales`
WHERE
  brand = 'TRAVELOKA'
  and country_id = 'ID'
  and issued_date is not null
), airy as (
SELECT distinct
  contact_email as ai_email,
  FIRST_VALUE(brand) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS first_brand_airy,
  FIRST_VALUE(booking_id) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS first_booking_id_airy,
  FIRST_VALUE(issued_date) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS first_issued_date_airy,
  LAST_VALUE(booking_id) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS last_booking_id_airy,
  LAST_VALUE(issued_date) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS last_issued_date_airy,
  COUNT(booking_id) OVER(PARTITION BY contact_email) AS num_issued_airy,
  COUNT(CASE
      WHEN coupon_amount < 0 THEN booking_id END) OVER(PARTITION BY contact_email) AS num_issued_coupon_airy,
  SUM(total_provider_fare) OVER(PARTITION BY contact_email) AS total_gbv_airy,
  AVG(total_provider_fare / num_room_night) OVER(PARTITION BY contact_email) AS avg_adr_airy,
  SUM(total_purchase_fare) OVER(PARTITION BY contact_email) AS total_spent_airy,
  SUM(total_commission) OVER(PARTITION BY contact_email) AS total_commission_airy,
  SUM(total_premium) OVER(PARTITION BY contact_email) AS total_premium_airy,
  SUM(coupon_amount) OVER(PARTITION BY contact_email) AS total_coupon_amount_airy,
  COUNT(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN booking_id END) OVER(PARTITION BY contact_email) AS num_issued_2018_airy,
  COUNT(CASE
      WHEN coupon_amount < 0 AND issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN booking_id END) OVER(PARTITION BY contact_email) AS num_issued_coupon_2018_airy,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN total_provider_fare END) OVER(PARTITION BY contact_email) AS total_gbv_2018_airy,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN total_purchase_fare END) OVER(PARTITION BY contact_email) AS total_spent_2018_airy,
      SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN total_commission END) OVER(PARTITION BY contact_email) AS total_commission_2018_airy,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN total_premium END) OVER(PARTITION BY contact_email) AS total_premium_2018_airy,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN coupon_amount END) OVER(PARTITION BY contact_email) AS total_coupon_amount_2018_airy,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN num_room_night END) OVER(PARTITION BY contact_email) AS num_room_night_2018_airy,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN num_room END) OVER(PARTITION BY contact_email) AS num_room_2018_airy,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN num_night END) OVER(PARTITION BY contact_email) AS num_night_2018_airy,
  PERCENTILE_CONT(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN DATE_DIFF(check_in_date, date(issued_date), DAY) END, 0.5) OVER(PARTITION BY contact_email) AS median_booking_windows_2018_airy
FROM
  `tvlk-data-accom-dev.accom_multibrand.fact_accommodation_sales`
WHERE
  case when brand = 'AIRY' and (affiliate_id not in ('traveloka', 'pegipegi') or affiliate_id is null) then true else false end
  and country_id = 'ID'
  and issued_date is not null
), pegipegi as (
SELECT distinct
  contact_email as pg_email,
  FIRST_VALUE(brand) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS first_brand_pegipegi,
  FIRST_VALUE(booking_id) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS first_booking_id_pegipegi,
  FIRST_VALUE(issued_date) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS first_issued_date_pegipegi,
  LAST_VALUE(booking_id) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS last_booking_id_pegipegi,
  LAST_VALUE(issued_date) OVER(PARTITION BY contact_email ORDER BY issued_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS last_issued_date_pegipegi,
  COUNT(booking_id) OVER(PARTITION BY contact_email) AS num_issued_pegipegi,
  COUNT(CASE
      WHEN coupon_amount < 0 THEN booking_id END) OVER(PARTITION BY contact_email) AS num_issued_coupon_pegipegi,
  SUM(total_provider_fare) OVER(PARTITION BY contact_email) AS total_gbv_pegipegi,
  AVG(total_provider_fare / num_room_night) OVER(PARTITION BY contact_email) AS avg_adr_pegipegi,
  SUM(total_purchase_fare) OVER(PARTITION BY contact_email) AS total_spent_pegipegi,
  SUM(total_commission) OVER(PARTITION BY contact_email) AS total_commission_pegipegi,
  SUM(total_premium) OVER(PARTITION BY contact_email) AS total_premium_pegipegi,
  SUM(coupon_amount) OVER(PARTITION BY contact_email) AS total_coupon_amount_pegipegi,
  COUNT(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN booking_id END) OVER(PARTITION BY contact_email) AS num_issued_2018_pegipegi,
  COUNT(CASE
      WHEN coupon_amount < 0 AND issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN booking_id END) OVER(PARTITION BY contact_email) AS num_issued_coupon_2018_pegipegi,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN total_provider_fare END) OVER(PARTITION BY contact_email) AS total_gbv_2018_pegipegi,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN total_purchase_fare END) OVER(PARTITION BY contact_email) AS total_spent_2018_pegipegi,
      SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN total_commission END) OVER(PARTITION BY contact_email) AS total_commission_2018_pegipegi,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN total_premium END) OVER(PARTITION BY contact_email) AS total_premium_2018_pegipegi,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN coupon_amount END) OVER(PARTITION BY contact_email) AS total_coupon_amount_2018_pegipegi,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN num_room_night END) OVER(PARTITION BY contact_email) AS num_room_night_2018_pegipegi,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN num_room END) OVER(PARTITION BY contact_email) AS num_room_2018_pegipegi,
  SUM(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN num_night END) OVER(PARTITION BY contact_email) AS num_night_2018_pegipegi,
  PERCENTILE_CONT(CASE
      WHEN issued_date >= TIMESTAMP('2017-12-31 17:00:00') THEN DATE_DIFF(check_in_date, date(issued_date), DAY) END, 0.5) OVER(PARTITION BY contact_email) AS median_booking_windows_2018_pegipegi
FROM
  `tvlk-data-accom-dev.accom_multibrand.fact_accommodation_sales`
WHERE
  brand = 'PEGIPEGI'
  and country_id = 'ID'
  and issued_date is not null
), final as (
select *
from overall
left join traveloka on overall.contact_email = traveloka.tvl_email
left join airy on overall.contact_email = airy.ai_email
left join pegipegi on overall.contact_email = pegipegi.pg_email)
select * except(tvl_email, ai_email, pg_email) from final
