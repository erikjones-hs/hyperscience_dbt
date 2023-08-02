select 

date,
session_default_channel_group as channel_grouping,
conversions,
new_users,
sessions,
total_users,
engaged_sessions,
screen_page_views

from {{ source('google_analytics_4', 'acquisition_report_version_2') }}
order by date desc