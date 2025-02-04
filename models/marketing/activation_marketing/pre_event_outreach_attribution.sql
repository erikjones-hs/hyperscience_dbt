/* 
this workflow is intended to set attribution for pre event outreach activities
it is tied to a Hightouch sync that sends outbound http requests
Hubspot workflows are set up with webhook enrollment and branching to determine the correct event
*/

select
    email
    ,campaign_id
    ,created_date as campaign_added_date
from {{ ref('int_sf_campaign_member')}}
where
    type in ('Lead','Contact')
    and email is not null
    and campaign_id in (
        '701Pm00000Y27wMIAR'
    )