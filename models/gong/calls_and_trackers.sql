{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'GONG'
)
}}

with calls as (
select * from {{ref('calls')}}
),

trackers as (
select * from {{ref('trackers')}}
),

calls_and_trackers as (
select 
c.*,
t.tracker_id,
t.tracker_name,
t.tracker_type,
t.keywords,
t.tracker_count,
t.tracker_category,
t.tracker_specific,
case when t.tracker_count > 0 then 1 else 0 end as call_had_tracker_mention
from calls c
left join trackers t on (c.CONVERSATION_KEY = t.CONVERSATION_KEY)
)

select * from calls_and_trackers


