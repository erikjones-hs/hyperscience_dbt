select 

date,
session_campaign_name as utm_campaign,
landing_page,
session_default_channel_grouping as channel_grouping,
session_source_medium as source_medium,
split_part(session_source_medium, ' / ', 1) as utm_source,
split_part(session_source_medium, ' / ', 2) as utm_medium,
conversions,
new_users,
sessions,
total_users,
event_count as events,
engaged_sessions,
screen_page_views

from {{ source('google_analytics_4', 'acquisition_report') }}