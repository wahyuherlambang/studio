select
  raw.*,
  last_52week.num_unique_profile as last_52week_num_unique_profile,
  last_52week.num_booking as last_52week_num_booking,
  last_52week.num_adult as last_52week_num_adult,
  last_52week.num_child as last_52week_num_child,
  last_52week.num_infant as last_52week_num_infant,
  last_52week.total_gbv as last_52week_total_gbv,
  last_52week.total_coupon_amount as last_52week_total_coupon_amount,
  last_52week.coupon_rate as last_52week_coupon_rate,
  last_52week.coupon_per_booking as last_52week_coupon_per_booking,
  last_52week.coupon_per_gbv as last_52week_coupon_per_gbv,
  last_52week.spend_per_customer as last_52week_spend_per_customer,
  last_52week.trx_per_customer as last_52week_trx_per_customer,
  last_52week.pax_per_customer as last_52week_pax_per_customer,
  last_52week.ROI as last_52week_ROI,
  last_12week.num_unique_profile as last_12week_num_unique_profile,
  last_12week.num_booking as last_12week_num_booking,
  last_12week.num_adult as last_12week_num_adult,
  last_12week.num_child as last_12week_num_child,
  last_12week.num_infant as last_12week_num_infant,
  last_12week.total_gbv as last_12week_total_gbv,
  last_12week.total_coupon_amount as last_12week_total_coupon_amount,
  last_12week.coupon_rate as last_12week_coupon_rate,
  last_12week.coupon_per_booking as last_12week_coupon_per_booking,
  last_12week.coupon_per_gbv as last_12week_coupon_per_gbv,
  last_12week.spend_per_customer as last_12week_spend_per_customer,
  last_12week.trx_per_customer as last_12week_trx_per_customer,
  last_12week.pax_per_customer as last_12week_pax_per_customer,
  last_12week.ROI as last_12week_ROI,
  last_4week.num_unique_profile as last_4week_num_unique_profile,
  last_4week.num_booking as last_4week_num_booking,
  last_4week.num_adult as last_4week_num_adult,
  last_4week.num_child as last_4week_num_child,
  last_4week.num_infant as last_4week_num_infant,
  last_4week.total_gbv as last_4week_total_gbv,
  last_4week.total_coupon_amount as last_4week_total_coupon_amount,
  last_4week.coupon_rate as last_4week_coupon_rate,
  last_4week.coupon_per_booking as last_4week_coupon_per_booking,
  last_4week.coupon_per_gbv as last_4week_coupon_per_gbv,
  last_4week.spend_per_customer as last_4week_spend_per_customer,
  last_4week.trx_per_customer as last_4week_trx_per_customer,
  last_4week.pax_per_customer as last_4week_pax_per_customer,
  last_4week.ROI as last_4week_ROI
from
(
  select
    *,
    DATE_SUB(week, INTERVAL 52 week) as last_52week,
    DATE_SUB(week, INTERVAL 12 week) as last_12week,
    DATE_SUB(week, INTERVAL 4 week) as last_4week
  from
    `tvlk-data-multibrand-prod.multibrand_flight.FACT_MULTI_BRAND_TMP`
  order by 1,2
) raw
left join
  `tvlk-data-multibrand-prod.multibrand_flight.FACT_MULTI_BRAND_TMP` last_52week
  on raw.brand = last_52week.brand
    and raw.last_52week = last_52week.week
left join
  `tvlk-data-multibrand-prod.multibrand_flight.FACT_MULTI_BRAND_TMP` last_12week
  on raw.brand = last_12week.brand
    and raw.last_12week = last_12week.week
left join
  `tvlk-data-multibrand-prod.multibrand_flight.FACT_MULTI_BRAND_TMP` last_4week
  on raw.brand = last_4week.brand
    and raw.last_4week = last_4week.week
order by 1,2
