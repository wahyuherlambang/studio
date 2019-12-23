 with coupon as (
  select
  order_entries.item_id.id as booking_id
  , case
      when LOWER(JSON_EXTRACT_SCALAR(data, '$[orderEntries][2][itemId][kind]')) = 'voucher'
        then JSON_EXTRACT_SCALAR(data, '$[orderEntries][2][itemId][scope]')
      end as coupon_code_1
  , case
      when LOWER(JSON_EXTRACT_SCALAR(data, '$[orderEntries][3][itemId][kind]')) = 'voucher'
        then JSON_EXTRACT_SCALAR(data, '$[orderEntries][3][itemId][scope]')
      end as coupon_code_2
  , case
      when LOWER(JSON_EXTRACT_SCALAR(data, '$[orderEntries][4][itemId][kind]')) = 'voucher'
        then JSON_EXTRACT_SCALAR(data, '$[orderEntries][4][itemId][scope]')
      end as coupon_code_3
  , case
      when LOWER(JSON_EXTRACT_SCALAR(data, '$[orderEntries][2][itemId][kind]')) = 'voucher'
        then cast(JSON_EXTRACT(data, '$[orderEntries][2][amount][amount]') as int64)
      end as coupon_amount_1
  , case
      when LOWER(JSON_EXTRACT_SCALAR(data, '$[orderEntries][3][itemId][kind]')) = 'voucher'
        then cast(JSON_EXTRACT(data, '$[orderEntries][3][amount][amount]') as int64)
      end as coupon_amount_2
  , case
      when LOWER(JSON_EXTRACT_SCALAR(data, '$[orderEntries][4][itemId][kind]')) = 'voucher'
        then cast(JSON_EXTRACT(data, '$[orderEntries][4][amount][amount]') as int64)
      end as coupon_amount_3
from `tvlk-realtime.mongo.payment_commerce_user_invoice_hour_1`
where
(_PARTITIONTIME BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 9 * 24 HOUR) AND CURRENT_TIMESTAMP()
  OR _PARTITIONTIME IS NULL)
  )
  
  select * from coupon
  where coupon_amount_1 is not null or coupon_amount_2 is not null or coupon_amount_3 is not null
