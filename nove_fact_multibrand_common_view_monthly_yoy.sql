select
  raw.*,
  last_12month.num_unique_profile as last_12month_num_unique_profile,
  last_12month.num_booking as last_12month_num_booking,
  last_12month.num_adult as last_12month_num_adult,
  last_12month.num_child as last_12month_num_child,
  last_12month.num_infant as last_12month_num_infant,
  last_12month.total_gbv as last_12month_total_gbv,
  last_12month.total_coupon_amount as last_12month_total_coupon_amount,
  last_12month.booking_with_coupon_ratio as last_12month_booking_with_coupon_ratio,
  last_12month.coupon_per_booking as last_12month_coupon_per_booking,
  last_12month.coupon_per_gbv as last_12month_coupon_per_gbv,
  last_12month.spend_per_customer as last_12month_spend_per_customer,
  last_12month.trx_per_customer as last_12month_trx_per_customer,
  last_12month.pax_per_customer as last_12month_pax_per_customer,
  last_12month.ROI as last_12month_ROI,
  last_4month.num_unique_profile as last_4month_num_unique_profile,
  last_4month.num_booking as last_4month_num_booking,
  last_4month.num_adult as last_4month_num_adult,
  last_4month.num_child as last_4month_num_child,
  last_4month.num_infant as last_4month_num_infant,
  last_4month.total_gbv as last_4month_total_gbv,
  last_4month.total_coupon_amount as last_4month_total_coupon_amount,
  last_4month.booking_with_coupon_ratio as last_4month_booking_with_coupon_ratio,
  last_4month.coupon_per_booking as last_4monthcoupon_per_booking,
  last_4month.coupon_per_gbv as last_4month_coupon_per_gbv,
  last_4month.spend_per_customer as last_4month_spend_per_customer,
  last_4month.trx_per_customer as last_4month_trx_per_customer,
  last_4month.pax_per_customer as last_4month_pax_per_customer,
  last_4month.ROI as last_4month_ROI,
  last_1month.num_unique_profile as last_1month_num_unique_profile,
  last_1month.num_booking as last_1month_num_booking,
  last_1month.num_adult as last_1month_num_adult,
  last_1month.num_child as last_1month_num_child,
  last_1month.num_infant as last_1month_num_infant,
  last_1month.total_gbv as last_1month_total_gbv,
  last_1month.total_coupon_amount as last_1month_total_coupon_amount,
  last_1month.booking_with_coupon_ratio	 as last_1month_booking_with_coupon_ratio,
  last_1month.coupon_per_booking as last_1month_coupon_per_booking,
  last_1month.coupon_per_gbv as last_1month_coupon_per_gbv,
  last_1month.spend_per_customer as last_1month_spend_per_customer,
  last_1month.trx_per_customer as last_1month_trx_per_customer,
  last_1month.pax_per_customer as last_1month_pax_per_customer,
  last_1month.ROI as last_1month_ROI
from
(
  select
    *,
    DATE_SUB(month, INTERVAL 12 month) as last_12month,
    DATE_SUB(month, INTERVAL 4 month) as last_4month,
    DATE_SUB(month, INTERVAL 1 month) as last_1month
  from
    `tvlk-data-multibrand-prod.multibrand_flight.FACT_MULTI_BRAND_TMP`
  order by 1,2
) raw
left join
  `tvlk-data-multibrand-prod.multibrand_flight.FACT_MULTI_BRAND_TMP` last_12month
  on raw.brand = last_12month.brand
    and raw.last_12month = last_12month.month
left join
  `tvlk-data-multibrand-prod.multibrand_flight.FACT_MULTI_BRAND_TMP` last_4month
  on raw.brand = last_4month.brand
    and raw.last_4month = last_4month.month
left join
  `tvlk-data-multibrand-prod.multibrand_flight.FACT_MULTI_BRAND_TMP` last_1month
  on raw.brand = last_1month.brand
    and raw.last_1month = last_1month.month
order by 1,2
