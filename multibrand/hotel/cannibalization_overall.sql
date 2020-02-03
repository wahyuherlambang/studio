SELECT
  first_brand,
  COUNT(DISTINCT CASE WHEN num_issued_traveloka > 0 THEN contact_email END) AS num_email_traveloka,
  COUNT(DISTINCT CASE WHEN num_issued_pegipegi > 0 THEN contact_email END) AS num_email_pegipegi,
  COUNT(DISTINCT CASE WHEN num_issued_airy > 0 THEN contact_email END) AS num_email_airy,
  AVG((total_gbv_traveloka - total_coupon_amount_traveloka) / num_issued_traveloka) AS avg_gbv_traveloka,
  AVG((total_gbv_pegipegi - total_coupon_amount_pegipegi) / num_issued_pegipegi) AS avg_gbv_pegipegi,
  AVG((total_gbv_airy - total_coupon_amount_airy) / num_issued_airy) AS avg_gbv_airy
FROM
  `tvlk-data-accom-dev.accom_multibrand.accom_multibrand_customer_master_2_vw`
WHERE
  contact_email NOT IN ( 'WcpgSFsgAMZ2jpt8Ly/1wlrOVUKso6//3x3I4BtT9OB7xRumpyZml8Lbjm5MrSPskvC7WjWQHQlGLpeOlUFwgA==',
    'xiMeFj/0cch/A6mOhVgBIwq6ari+vqYEqnms1Xy5UXAklsN50o4R0OPJmAbPeadxxVmYngorQD2AVqO/fTjypw==',
    'xE8GPJwM4+b+YQpZ3Wx1Q+6xJNq1YMIbT0iKZcTLq803923lCOBVAsnhWwbtcQ+GJ/ZuMN4bcKQfjylyXEWakg==' )
GROUP BY
  1
