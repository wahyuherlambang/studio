WITH
  traveloka AS (
  SELECT
    contact_email,
    'TRAVELOKA' AS brand,
    CASE
      WHEN num_issued_2018_traveloka > 0 THEN 1
      ELSE 0
    END AS use_traveloka,
    CASE
      WHEN num_issued_2018_airy > 0 OR num_issued_2018_pegipegi > 0 THEN 1
      ELSE 0
    END AS multibrand,
    num_issued_2018_traveloka as num_issued_2018,
    total_gbv_2018_traveloka as total_gbv_2018,
    total_coupon_amount_2018_traveloka / nullif(num_issued_coupon_2018_traveloka, 0) as avg_coupon_2018
  FROM
    `tvlk-data-accom-dev.accom_multibrand.accom_multibrand_customer_master`
  WHERE
    num_issued_2018_traveloka > 0 ),
  airy AS (
  SELECT
    contact_email,
    'AIRY' AS brand,
    CASE
      WHEN num_issued_2018_traveloka > 0 THEN 1
      ELSE 0
    END AS use_traveloka,
    CASE
      WHEN num_issued_2018_traveloka > 0 OR num_issued_2018_pegipegi > 0 THEN 1
      ELSE 0
    END AS multibrand,
    num_issued_2018_airy as num_issued_2018,
    total_gbv_2018_airy as total_gbv_2018,
    total_coupon_amount_2018_airy / nullif(num_issued_coupon_2018_airy, 0) as avg_coupon_2018    
  FROM
    `tvlk-data-accom-dev.accom_multibrand.accom_multibrand_customer_master`
  WHERE
    num_issued_2018_airy > 0 ),
  pegipegi AS (
  SELECT
    contact_email,
    'PEGIPEGI' AS brand,
    CASE
      WHEN num_issued_2018_traveloka > 0 THEN 1
      ELSE 0
    END AS use_traveloka,
    CASE
      WHEN num_issued_2018_airy > 0 OR num_issued_2018_traveloka > 0 THEN 1
      ELSE 0
    END AS multibrand,
    num_issued_2018_pegipegi as num_issued_2018,
    total_gbv_2018_pegipegi as total_gbv_2018,
    total_coupon_amount_2018_pegipegi / nullif(num_issued_coupon_2018_pegipegi, 0) as avg_coupon_2018
  FROM
    `tvlk-data-accom-dev.accom_multibrand.accom_multibrand_customer_master`
  WHERE
    num_issued_2018_pegipegi > 0 ),
  base AS (
  SELECT
    *
  FROM
    traveloka
  UNION ALL
  SELECT
    *
  FROM
    airy
  UNION ALL
  SELECT
    *
  FROM
    pegipegi)
SELECT
  brand,
  use_traveloka,
  multibrand,
  COUNT(DISTINCT contact_email) AS num_email,
  COUNT(contact_email),
  SUM(total_gbv_2018) / COUNT(contact_email) as gbv_per_user,
  avg(total_gbv_2018 / num_issued_2018) as avg_gbv_per_user,
  SUM(total_gbv_2018) AS gbv,
  SUM(num_issued_2018) AS num_issued,
  SUM(num_issued_2018) / COUNT(contact_email) as issued_per_user,
  avg(avg_coupon_2018) as avg_coupon_per_user
FROM
  base
where contact_email not in (
'WcpgSFsgAMZ2jpt8Ly/1wlrOVUKso6//3x3I4BtT9OB7xRumpyZml8Lbjm5MrSPskvC7WjWQHQlGLpeOlUFwgA=='
, 'xiMeFj/0cch/A6mOhVgBIwq6ari+vqYEqnms1Xy5UXAklsN50o4R0OPJmAbPeadxxVmYngorQD2AVqO/fTjypw=='
, 'xE8GPJwM4+b+YQpZ3Wx1Q+6xJNq1YMIbT0iKZcTLq803923lCOBVAsnhWwbtcQ+GJ/ZuMN4bcKQfjylyXEWakg=='
)
GROUP BY
  1,
  2,
  3
ORDER BY
  1,
  2,
  3
