WITH
  base AS (
  SELECT
    contact_email,
    CASE
      WHEN CASE
      WHEN num_issued_2018_traveloka > 0 THEN 1
      ELSE 0
    END +
    CASE
      WHEN num_issued_2018_airy > 0 THEN 1
      ELSE 0
    END +
    CASE
      WHEN num_issued_2018_pegipegi > 0 THEN 1
      ELSE 0
    END > 1 THEN 1
      ELSE 0
    END AS multibrand_user,
    num_issued_2018,
    num_issued_coupon_2018,
    abs(total_coupon_amount_2018) / nullif(num_issued_coupon_2018,
      0) AS avg_coupon_2018
  FROM
    `tvlk-data-accom-dev.accom_multibrand.accom_multibrand_customer_master`
  WHERE
    num_issued_2018 > 0
    AND contact_email NOT IN ( 'WcpgSFsgAMZ2jpt8Ly/1wlrOVUKso6//3x3I4BtT9OB7xRumpyZml8Lbjm5MrSPskvC7WjWQHQlGLpeOlUFwgA==',
      'xiMeFj/0cch/A6mOhVgBIwq6ari+vqYEqnms1Xy5UXAklsN50o4R0OPJmAbPeadxxVmYngorQD2AVqO/fTjypw==',
      'xE8GPJwM4+b+YQpZ3Wx1Q+6xJNq1YMIbT0iKZcTLq803923lCOBVAsnhWwbtcQ+GJ/ZuMN4bcKQfjylyXEWakg==' ) )
SELECT
  multibrand_user,
  AVG(avg_coupon_2018) AS avg_coupon_per_user,
  AVG(num_issued_coupon_2018 / num_issued_2018) AS avg_prop_coupon_per_user
FROM
  base
GROUP BY
  1
