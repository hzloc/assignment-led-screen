 create materialized view if not exists rate_ranked_per_ccy_ordered_by_most_recent as (
SELECT
	(to_timestamp(event_time / 1000) at TIME zone 'America/New_York') as event_time_ny_tz,
	rate,
	-- ISO 4217 assigns 3 digit for currency
	UPPER(left(ccy_couple, 3) || '/' || right(ccy_couple, 3)) as ccy_couple,
	rank() over (partition by ccy_couple ORDER BY event_time DESC, rate DESC) AS most_recent
FROM
	rates r
WHERE
	-- rate 0 would not be logical
	rate != 0);
REFRESH MATERIALIZED VIEW rate_ranked_per_ccy_ordered_by_most_recent;

CREATE materialized VIEW IF NOT EXISTS conversion_rates_led_table_mv AS
    (WITH recent_rates as (
        select
                event_time_ny_tz,
                rate,
                ccy_couple,
                most_recent
        from
                rate_ranked_per_ccy_ordered_by_most_recent
        where
                event_time_ny_tz >= (now() - interval '30 sec') at TIME zone 'America/New_York'
                and
                most_recent = 1
            ),
    yesterdays_rates as
        (
        select DISTINCT
            event_time_ny_tz,
            rate,
            ccy_couple
    from
        rate_ranked_per_ccy_ordered_by_most_recent
    where
        -- Yesterday at 17:00:00
        event_time_ny_tz = (current_date - interval '1 day' + '17:00:00')
        )
select
rrt.ccy_couple,
case
    when rrt.rate != 0 then TO_CHAR(rrt.rate, 'FM9999999999990.00000')
    else 'Not available'
end as rate,
case
    when yr.rate is null
    or yr.rate = 0 then 'Not available'
    when rrt.rate is null
    or rrt.rate = 0 then 'Not available'
    else TO_CHAR((rrt.rate - yr.rate) / yr.rate * 100,
    'FM9999999990.000%')
end as change
from
recent_rates rrt
left join yesterdays_rates yr on
yr.ccy_couple = rrt.ccy_couple);
REFRESH MATERIALIZED VIEW conversion_rates_led_table_mv;
