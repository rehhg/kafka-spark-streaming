select
    from_unixtime(unix_timestamp()) as curr_time,
    t.city as city,
    t.currency as currency_code,
    sum(amount) as batch_value
from treasury_stream t
group by
    t.city,
    t.currency