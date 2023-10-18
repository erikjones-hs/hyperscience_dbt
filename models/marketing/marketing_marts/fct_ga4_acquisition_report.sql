with data as (


    select *
    from {{ source('google_analytics_4', 'acquisition_report_version_2') }}

)

select 

date,
session_source_medium,
--session_default_channel_group as channel_grouping,
conversions,
new_users,
sessions,
total_users,
engaged_sessions,
screen_page_views

from data
where session_source_medium like '%google%'
order by date desc