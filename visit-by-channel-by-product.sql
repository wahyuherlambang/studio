SELECT
   DATE(DATETIME_TRUNC(DATETIME_ADD(v1.kafka_publish_timestamp,
         INTERVAL 7 HOUR),
       MONTH)) AS Month,
  COALESCE(map.level_1_group,
    'Brand') AS source_group,
  map.product as map_product, v1.product as v1_product,
  COUNT(v1._id) AS num_visit
FROM
  `tvlk-realtime.nrtprod.tvlk_visit` AS v1
LEFT JOIN
    `tvlk-realtime.gdrive.marketing_channel_hierarchy` AS map
ON
  v1.source = map.channels
WHERE
  v1.country IN ('ID',
    'MY',
    'SG',
    'TH',
    'VN',
    'PH')
  AND v1.interface IN ('desktop',
    'mobile',
    'mobile-apps',
    'mobile-android',
    'mobile-iOS')
  AND DATE(map._PARTITIONTIME) = '2019-11-25' 
  --AND DATE(v1._PARTITIONTIME) >= '2019-01-01'
GROUP BY
  1,
  2,
  3,
  4
