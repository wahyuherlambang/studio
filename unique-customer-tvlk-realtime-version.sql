select date_trunc(date(kafka_publish_timestamp), month) as issued_date, count(distinct contact_email) as unique_cust from `tvlk-realtime.nrtprod.flight_issued_new`
group by 1
