SELECT
  TIMESTAMP_TRUNC(TIMESTAMP_ADD(issued_date, INTERVAL 7 HOUR), MONTH) AS month,
  brand,
  COUNT(booking_id) AS num_issued
FROM
  `tvlk-data-accom-dev.accom_multibrand.fact_accommodation_sales`
WHERE
  issued_date >= TIMESTAMP('2017-11-30 17:00:00')
  and issued_date < TIMESTAMP('2018-12-31 17:00:00')
  and case when brand = 'AIRY' and affiliate_id in ('traveloka', 'pegipegi') then false else true end
GROUP BY
  1,
  2
ORDER BY
  1,
  2
