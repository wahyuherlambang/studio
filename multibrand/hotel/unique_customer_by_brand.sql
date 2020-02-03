SELECT
  TIMESTAMP_TRUNC(TIMESTAMP_ADD(s.issued_date, INTERVAL 7 HOUR), MONTH) AS month,
  s.brand,
  case 
  when 
    cm_tv.first_booking_id_traveloka is not null
  or
    cm_ai.first_booking_id_airy is not null
  or
    cm_pg.first_booking_id_pegipegi is not null
  then 1 else 0 end as is_new_customer,
  COUNT(DISTINCT s.contact_email) AS num_email
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
  AND s.issued_date < TIMESTAMP('2018-12-31 17:00:00')
  and case when s.brand = 'AIRY' and s.affiliate_id in ('traveloka', 'pegipegi') then false else true end
GROUP BY
  1,
  2,
  3
ORDER BY
  1,
  2,
  3
