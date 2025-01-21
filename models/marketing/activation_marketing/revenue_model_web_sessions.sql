select
    date as month
    ,concat(
        case when month(date) in (1,2) then year(date) - 1
             else year(date) end
        ,' '
        ,case when month(date) in (3,4,5) then 'Q1'
              when month(date) in (6,7,8) then 'Q2'
              when month(date) in (9,10,11) then 'Q3'
              else 'Q4' end
    ) as fiscal_quarter
    ,channel
    ,sessions
from {{ ref('stg_hubspot_web_sessions_monthly')}}
unpivot (
    sessions for channel in (direct_traffic, email_marketing, other_campaigns, paid_search, 
                            social_media, referrals, organic_search, paid_social)
)
where
    breakdown = 'sessions'
    and date >= '2024-05-01'
order by month, channel