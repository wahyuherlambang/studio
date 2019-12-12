SELECT
  *
FROM
(
SELECT
  'Airy' as brand,
  date_trunc(date(issued_date), month) as month,
  count(distinct contact_email) as num_unique_profile,
  count(booking_id) as num_booking,
  sum(num_adult) as num_adult,
  sum(num_child) as num_child,
  sum(num_infant) as num_infant,
  sum(total_fare) as total_gbv,
  sum(coupon_amount) as total_coupon_amount,
  sum(case when coupon_amount>0 then 1 else 0 end) / count(booking_id) as coupon_rate,
  sum(cast(coupon_amount as float64)) / count(booking_id) as coupon_per_booking,
  sum(cast(coupon_amount as float64)) / sum(total_fare) as coupon_per_gbv,
  sum(total_purchase_price) / count(distinct contact_email) as spend_per_customer,
  count(booking_id) / count(distinct contact_email) as trx_per_customer,
  sum(num_adult + num_child + num_infant) / count(distinct contact_email) as pax_per_customer,
  case when sum(coupon_amount) > 0 then sum(total_purchase_price)*1.0 / sum(coupon_amount) else 0 end as ROI
FROM `tvlk-data-multibrand-prod.multibrand_flight.FACT_AI_FLIGHT_SALES`
GROUP BY 1,2
)
UNION ALL
(
SELECT
  'PegiPegi' as brand,
  date_trunc(date(issued_date), month) as month,
  count(distinct contact_email) as num_unique_profile,
  count(booking_id) as num_booking,
  sum(num_adult) as num_adult,
  sum(num_child) as num_child,
  sum(num_infant) as num_infant,
  sum(total_fare) as total_gbv,
  sum(coupon_amount) as total_coupon_amount,
  sum(case when coupon_amount>0 then 1 else 0 end) / count(booking_id) as coupon_rate,
  sum(cast(coupon_amount as float64)) / count(booking_id) as coupon_per_booking,
  sum(cast(coupon_amount as float64)) / sum(total_fare) as coupon_per_gbv,
  sum(total_purchase_price) / count(distinct contact_email) as spend_per_customer,
  count(booking_id) / count(distinct contact_email) as trx_per_customer,
  sum(num_adult + num_child + num_infant) / count(distinct contact_email) as pax_per_customer,
  case when sum(coupon_amount) > 0 then sum(total_purchase_price)*1.0 / sum(coupon_amount) else 0 end as ROI
FROM `tvlk-data-multibrand-prod.multibrand_flight.FACT_PG_FLIGHT_SALES`
GROUP BY 1,2
)
UNION ALL
(
SELECT
  'Traveloka' as brand,
  date_trunc(date(issued_date), month) as month,
  count(distinct contact_email) as num_unique_profile,
  count(sales.booking_id) as num_booking,
  sum(num_adult) as num_adult,
  sum(num_child) as num_child,
  sum(num_infant) as num_infant,
  sum(total_fare) as total_gbv,
  sum(cast (coupon_amount as float64)) as total_coupon_amount,
  sum(case when cast(coupon_amount as float64)>0 then 1 else 0 end) / count(sales.booking_id) as coupon_rate,
  sum(cast(coupon_amount as float64)) / count(sales.booking_id) as coupon_per_booking,
  sum(cast(coupon_amount as float64)) / sum(total_fare) as coupon_per_gbv,
  sum(total_purchase_price) / count(distinct contact_email) as spend_per_customer,
  count(sales.booking_id) / count(distinct contact_email) as trx_per_customer,
  sum(num_adult + num_child + num_infant) / count(distinct contact_email) as pax_per_customer,
  case when sum(cast(coupon_amount as float64)) > 0 then sum(total_purchase_price)*1.0 / sum(cast(coupon_amount as float64)) else 0 end as ROI
FROM `tvlk-data-multibrand-prod.multibrand_flight.FACT_TV_FLIGHT_SALES` sales
join `tvlk-data-multibrand-prod.multibrand_flight.FACT_TV_FLIGHT_DATA_PII64` pii
on sales.booking_id = pii.booking_id
where
sales.Source_Airport_Country_Id = 'ID' and sales.Destination_Airport_Country_Id = 'ID' 
GROUP BY 1,2
)
ORDER BY 1,2
