with data as (


    select *
    from {{ source('google_analytics_4', 'acquisition_report_version_2') }}

)

select *
from data
order by date desc