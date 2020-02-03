WITH CURRENCY_RATE AS (
  select
    DISTINCT *
  from
    edw.fact_currency_conversion_rate
),
coupon as (
  select
    booking_id,
    coupon_currency_id,
    case when num_coupon > 1 then 'Multiple Coupon' else coupon_type end as coupon_type,
    sum(coupon_amount) as coupon
  from(
      select
        booking_id,
        coupon_currency_id,
        coupon_type,
        coupon_amount,
        count(coupon_type) over(partition by booking_id, coupon_currency_id) num_coupon
      from
        (
          select
            bc.booking_id,
            coupon_currency_id,
            coalesce(coupon_type, 'Others') as coupon_type,
            sum(coupon_amount) coupon_amount
          from
            edw.fact_commerce_booking_coupon bc
            left join (
              select
                distinct dc.coupon_code,
                coalesce(
                  case when cm.type = '#N/A ()' then 'Others' when cm.type = 'Purchased coupon' then 'Purchased Coupon' else cm.type end,
                  case when (
                    dc.owner = 'MARKETING'
                    and is_for_specific_user = 'true'
                  )
                  or (
                    regexp_replace(
                      dc.coupon_code,
                      '[A-Z]{2}[0-9]{2}[A-Z]{3}[0-9]{2}[A-Z][A-Z0-9]*',
                      'true'
                    ) = 'true'
                  ) then 'BTL Coupon' else 'Others' end
                ) as coupon_type
              from
                edw.dim_commerce_coupon dc
                left join holidw.accom_coupon_mapping cm on dc.coupon_code = cm."coupon code"
            ) dc on dc.coupon_code = bc.coupon_code
          where
            bc.booking_status = 'ISSUED'
            and coupon_remark not like 'Standard Chart%'
          group by
            1,
            2,
            3
        ) -- where coupon_type = 'ATL Coupon, BTL Coupon'
      group by
        1,
        2,
        3,
        4
    )
  group by
    1,
    2,
    3
)
select
  hb.booking_id,
  hb.issued_time,
  hb.cookie_id,
  hb.session_id,
  hb.device_id,
  hb.login_id,
  hb.interface,
  hb.country_id,
  hb.language_id,
  hb.currency_id,
  hb.actual_agent_issued_total_rate_currency_id as supply_currency_id,
  CASE WHEN hb.is_pay_at_hotel_issued = TRUE THEN 'PAY_AT_HOTEL' ELSE 'PAY_NOW' END payment_type,
  CASE when hb.country_id = 'ID'
  and g.country_name = 'Indonesia' then 'DOMESTIC' when hb.country_id = 'ID'
  and g.country_name != 'Indonesia' then 'INTERNATIONAL' when hb.country_id = 'MY'
  and g.country_name = 'Malaysia' then 'DOMESTIC' when hb.country_id = 'MY'
  and g.country_name != 'Malaysia' then 'INTERNATIONAL' when hb.country_id = 'PH'
  and g.country_name = 'Philippines' then 'DOMESTIC' when hb.country_id = 'PH'
  and g.country_name != 'Philippines' then 'INTERNATIONAL' when hb.country_id = 'SG'
  and g.country_name = 'Singapore' then 'DOMESTIC' when hb.country_id = 'SG'
  and g.country_name != 'Singapore' then 'INTERNATIONAL' when hb.country_id = 'TH'
  and g.country_name = 'Thailand' then 'DOMESTIC' when hb.country_id = 'TH'
  and g.country_name != 'Thailand' then 'INTERNATIONAL' when hb.country_id = 'VN'
  and g.country_name = 'Vietnam' then 'DOMESTIC' when hb.country_id = 'VN'
  and g.country_name != 'Vietnam' then 'INTERNATIONAL' else 'NA' end as travel_type,
  hb.type as trip_type,
  ab.affiliate_id,
  h.hotel_id,
  h.hotel_name,
  h.star_rating as hotel_star_rating,
  h.chain_name as hotel_chain_name,
  g.country_id as hotel_country_id,
  g.country_name as hotel_country_name,
  g.region_id as hotel_region_id,
  g.region_name as hotel_region_name,
  g.city_id as hotel_city_id,
  g.city_name as hotel_city_name,
  g.area_id as hotel_area_id,
  g.area_name as hotel_area_name,
  SPLIT_PART(hb.provider_id, '_', 1) as provider_group,
  hb.check_in_date,
  hb.check_out_date,
  hb.num_rooms,
  hb.num_of_nights,
  hb.num_rooms * hb.num_of_nights as num_room_night,
  hb.room_occupancy_num_adults as num_adult,
  hb.contact_person_title,
  coalesce(hb.contact_first_name, '') || ' ' || coalesce(hb.contact_last_name, '') as contact_full_name,
  hb.contact_email,
  hb.contact_phone,
  coalesce(hb.guest_first_name, '') || ' ' || coalesce(hb.guest_last_name, '') as guest_full_name,
  hb.payment_method,
  hb.actual_agent_issued_total_rate_total_fare as total_provider_fare,
  hb.agent_booked_total_rate_total_fare as total_booked_fare,
  hb.agent_booked_total_rate_total_fare + coalesce(c.coupon, 0) -- coupon
  + coalesce(pi.unique_code_amount, 0) as total_purchase_fare,
  ((hb.agent_booked_total_rate_total_fare)) - (
    hb.actual_agent_issued_total_rate_total_fare * coalesce(hc.exchange_rate, coalesce(hc2.exchange_rate, 1))
  ) as total_premium,
  hb.actual_agent_issued_total_rate_total_fare * case when hb.provider_id like '%traveloka%' then coalesce(tera.commission_percent, 0.15) when hb.provider_id like '%expedia%' then 0.075 when hb.provider_id like '%hotelbeds%' then 0 end - case when affiliate_id = 'airy-hotel' then hb.actual_agent_issued_total_rate_total_fare * 0.05 when affiliate_id = 'angkasa-pura-2-hotel' then hb.actual_agent_issued_total_rate_total_fare * 0.055 when affiliate_id = 'ctrip-hotel' then hb.actual_agent_issued_total_rate_total_fare * 0.1 else 0 end -- affiliate share
  as total_commission,
  coalesce(c.coupon, 0) as coupon_amount,
  hb.source
from
  edw.fact_hotel_booking hb
  join edw.dim_hotel h on hb.hotel_id = h.hotel_id
  join edw.dim_geo_region g on h.geo_region_id = g.geo_region_id
  left join (
    select
      affiliate_id,
      booking_id
    from
      edw.fact_hotel_affiliation_booking
  ) ab on ab.booking_id = hb.booking_id
  left join coupon c on c.booking_id = hb.booking_id
  and c.coupon_currency_id = hb.agent_booked_total_rate_currency_id
  left join edw.fact_payment_invoice as pi on pi.booking_id = hb.booking_id
  left join CURRENCY_RATE hc on hb.conversion_rate_id = hc.conversion_rate_id
  and hc.destination_currency_id = hb.agent_booked_total_rate_currency_id
  and hc.source_currency_id = hb.actual_agent_issued_total_rate_currency_id
  left join (
    select
      *
    from(
        select
          distinct source_currency_id,
          destination_currency_id,
          exchange_rate,
          modified_at,
          created_time,
          dense_rank() over(
            partition by destination_currency_id,
            source_currency_id
            order by
              modified_at desc,
              created_time desc
          ) as dr
        from
          edw.fact_currency_conversion_rate
      )
    where
      dr = 1
  ) hc2 on hc2.destination_currency_id = hb.agent_booked_total_rate_currency_id
  and hc2.source_currency_id = hb.actual_agent_issued_total_rate_currency_id
  left join (
    select
      reservation_id,
      commission_percent as actual_commission_percent,
      case when reduce_room_night_amend_type is not null then round(commission_percent / 100, 3) else 1 - round(actual_net_fare_total / fare_total, 4) end as commission_percent
    from
      edw.fact_tera_hotel_reservation
  ) as tera on tera.reservation_id = hb.provider_booking_id
where
  hb.booking_status = 'ISSUED'
  order by hb.booking_id
