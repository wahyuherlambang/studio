WITH

dim_region AS (
  SELECT
    hotel_id,
    global_address_country as country_id,
    --country_name,
    provider_region_id as region_id,
    --region_name,
    --city_id,
    global_address_city as city_name,
    --area_id,
    --area_name,
    --center_latitude AS geo_center_latitude,
    --center_longitude AS geo_center_longitude,
    --popularity AS geo_popularity_score,
    --seo_search_volume AS geo_seo_search_volume,
    --modified_at,
    _PARTITIONTIME,
    kafka_publish_timestamp
  FROM
    `tvlk-realtime.nrtprod.hotel_data_providers`
  WHERE
    
    _PARTITIONTIME >= "2019-12-01"
    AND _PARTITIONTIME < "2019-12-31"),
    
    
  dim_region_latest AS 
(
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY hotel_id ORDER BY kafka_publish_timestamp DESC) AS row_
  FROM
    dim_region
  WHERE
    _PARTITIONTIME = (SELECT MAX(_PARTITIONTIME) FROM dim_region)
),

dim_region_final as
(
  SELECT
    * EXCEPT(row_)
  FROM
    dim_region_latest
  WHERE
    row_ = 1
),

  dim_hotel AS 
(
  SELECT
    hotel_id,
    name as hotel_name,
    accommodation_type,
    --quicktab_tags,
    IF(brand_id IN (
      '207', # Others
      '1001', # Other
      '1005', # Other
      '1007', # Other
      '1012', # Other
      '1019', # null
      '510000', # Independent Hotels
      '510001', # 1 Hotels
      '3100000000132' # Airy Rooms
      ), NULL, brand_id) AS hotel_brand_id,
    --brand_name AS hotel_brand_name,
    IF(chain_id IN (
      '0', # INDEPENDENT HOTELS
      '1000000000371', # OYO Rooms
      '3000000000093', # Airy Rooms
      '200000000060214561', # Alpha
      '200000000060214564' # Featured Superstars
      ), NULL, chain_id) AS hotel_chain_id,
    --chain_name AS hotel_chain_name,
    --geo_region_id AS hotel_geo_id,
    latitude AS hotel_latitude,
    longitude AS hotel_longitude,
    properties_built_year AS hotel_built_year,
    properties_last_renovated_year as hotel_last_renovated_year,
    properties_check_in_time AS hotel_check_in_time,
    properties_check_out_time AS hotel_check_out_time,
    properties_num_bars AS hotel_num_bars,
    properties_num_floors AS hotel_num_floors,
    properties_num_restaurants AS hotel_num_restaurants,
    properties_num_rooms as hotel_num_rooms,
    parking_feetype AS hotel_parking_fee_type,
    properties_parking_type AS hotel_parking_type,
    star_rating AS hotel_star_rating,
    --popularity_score as hotel_popularity_score,
    --seo_search_volume as hotel_seo_search_volume,
--     is_style_adventure_hotels,
--     is_style_airport_hotels,
--     is_style_backpacker_hotels,
--     is_style_boutique_hotels,
--     is_style_budget_hotels,
--     is_style_business_hotels,
--     is_style_conference_hotels,
--     is_style_family_hotels,
--     is_style_gay_friendly_hotels,
--     is_style_golf_hotels,
--     is_style_hip_hotels,
--     is_style_historic_hotels,
--     is_style_honeymoon_hotels,
--     is_style_long_stay_hotels,
--     is_style_luxury_hotels,
--     is_style_pet_friendly_hotels,
--     is_style_resort_hotels,
--     is_style_shopping_hotels,
--     is_style_single_hotels,
--     is_style_spa_hotels,
--     is_active_hotel,
    --modified_at,
    _PARTITIONTIME,
    kafka_publish_timestamp
  FROM
    `tvlk-realtime.nrtprod.hotel_data`
  WHERE
    _PARTITIONTIME >= "2019-12-01"
    AND _PARTITIONTIME < "2019-12-31"
),
       
  dim_hotel_latest AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY hotel_id ORDER BY kafka_publish_timestamp DESC) AS row_
  FROM
    dim_hotel
  WHERE
    _PARTITIONTIME = (SELECT MAX(_PARTITIONTIME) FROM dim_hotel)),
    
    
  dim_hotel_modified AS (
  SELECT
    dim_hotel_latest.* EXCEPT(row_),
    --geo.geo_name as hotel_geo_name,
    --geo.geo_type as hotel_geo_type,
    geo.country_id as hotel_country_id,
    --geo.country_name as hotel_country_name,
    geo.region_id as hotel_region_id,
    --geo.region_name as hotel_region_name,
    --geo.city_id as hotel_city_id,
    geo.city_name as hotel_city_name,
    --geo.area_id as hotel_area_id,
    --geo.area_name as hotel_area_name
  FROM
    dim_hotel_latest
  JOIN
    dim_region_final AS geo
  ON
    dim_hotel_latest.hotel_id = geo.hotel_id
  WHERE
    row_ = 1)
    
    
SELECT
  *,
  COUNT(IF(hotel_brand_id IS NOT NULL, hotel_id, NULL)) OVER(PARTITION BY hotel_brand_id) AS num_hotel_in_brand,
  COUNT(IF(hotel_chain_id IS NOT NULL, hotel_id, NULL)) OVER(PARTITION BY hotel_chain_id) AS num_hotel_in_chain
FROM
  dim_hotel_modified
