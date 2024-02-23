with events1 as (
  SELECT 
  timestamp_micros(event_timestamp) as event_timestamp,
  event_name,
  user_pseudo_id || cast(unnested_params.value.int_value as string) as user_session_id,
  traffic_source.source,
  traffic_source.medium,
  traffic_source.name as campaign
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  cross join unnest(event_params) as unnested_params
  WHERE unnested_params.key like 'ga_session_id' and event_name in ('session_start', 'add_to_cart', 'begin_checkout', 'purchase')),
  events_count as(
    select date(event_timestamp) as event_date,
    source,
    medium,
    campaign,
    count(distinct user_session_id) as user_sessions_count,
    count(distinct case when event_name = 'add_to_cart'then user_session_id end) as add_to_cart,
    count(distinct case when event_name = 'begin_checkout'then user_session_id end) as begin_checkout,
    count(distinct case when event_name = 'purchase'then user_session_id end) as purchase
    from events1 
    group by 1,2,3,4)
    select event_date,
    source,
    medium,
    campaign,
    user_sessions_count,
    round((add_to_cart / user_sessions_count) * 100, 2) || '\u0025' as visit_to_cart,
    round((begin_checkout / user_sessions_count) * 100, 2) || '\u0025' as visit_to_checkout,
    round((purchase / user_sessions_count) * 100, 2) || '\u0025' as visit_to_purchase
    from events_count
  limit 10;
