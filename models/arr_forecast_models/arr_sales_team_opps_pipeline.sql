{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'ARR_FORECAST'
)
}}

with pipeline as (
select distinct
account_id,
account_name,
opp_id,
opp_name,
opp_commit_status,
opportunity_owner,
owner_description,
CASE WHEN owner_description in ('EMEA Account Executive','EMEA VP','EMEA Sales Outreach','EMEA Central Europe - Account Executive',
                                'EMEA Central Europe - Channel Sales','EMEA Central Europe - Regional Director','EMEA North Europe - Account Executive',
                                'EMEA North Europe - Channel Sales','EMEA North Europe - Regional Director','EMEA - Sales AVP') then 'EMEA'
     WHEN owner_description in ('Account Manager','US Commercial - Account Executive','US Commercial - Regional Director') then 'Commercial'
     WHEN owner_description in ('ANZ - Channel Sales','ANZ - Regional Director','Global - Channel AVP') then 'APAC'
     WHEN owner_description in ('US Federal','US Federal - Account Executive','US SLED - Regional Director','US SLED - Account Executive','Global Federal - Account Executive',
                                'Global Public Sector - AVP','Global Federal - Channel Sales') then 'Federal'
     WHEN owner_description in ('US Account Executive - Tri-State','US Director - Tri-State','US AVP - Tri-State','US East - Regional Director','US East - Account Executive') then 'US East'
     WHEN owner_description in ('US West - Account Executive','US West - Regional Director', 'US - Sales VP') then 'US West'
     WHEN owner_description in ('US - Channel Sales','Global GSI - Channel Sales','Global GSP - Channel Sales') then 'Channel'
     ELSE 'Other' end as sales_team,
CASE WHEN opp_id = '0061R000014vnNlQAI' then 150000
     WHEN opp_id = '0061R000014yHlcQAE' then 368750
     else opp_net_new_arr end as opp_net_new_arr,
opp_stage_name,
opp_arr,
opp_close_dte
from "DEV"."SALES"."SALESFORCE_AGG_OPPORTUNITY"
where opp_stage_name not in ('Opp DQed', 'Closed Won')
and opp_revenue_type not in ('Renewal','License Overage')
and opportunity_owner not in ('Eli Berman')
and opp_commit_status in ('Committed','Best Case','Visible Opportunity','Pipeline')
and opp_name not in ('Mutual of Omaha-2020-21 auto renew')
and to_date(opp_close_dte) >= '2021-03-01'
and to_date(opp_close_dte) <= '2023-02-28'
)

select * from pipeline