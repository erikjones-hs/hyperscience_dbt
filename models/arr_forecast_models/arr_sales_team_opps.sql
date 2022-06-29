{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'ARR_FORECAST'
)
}}

/* Closed Won Opportunities for computing Sales Actuals by team */
/* Using FCT_ARR_OPP SO IT MATCHES WITH OUR KPI DASHBOARD */
with opps as (
select * from {{ ref('fct_arr_opp') }}
),

/* Need to pull in opportunity owner to assign to sales team */
owner as (
select distinct
opp_id,
opp_name,
account_id,
account_name,
CASE WHEN opp_id = '0061R000016kGCyQAM' then '0051R00000Hf8Z8QAJ' else opp_owner_id end as opp_owner_id,
CASE WHEN opp_id = '0061R000016kGCyQAM' then 'Bryan Bledsoe' else opportunity_owner end as opportunity_owner,
CASE WHEN opp_id = '0061R000016kGCyQAM' then 'US - Sales VP' else owner_description end as owner_description,
opp_closed_won_dte
from {{ ref('agg_opportunity_incremental') }}
where to_date(date_ran) = dateadd(day,-1,(to_date(current_date)))
),

/* Combining Opps and Opp Owner */
fct_opp_owner as (
select 
opp.date_month,
opp.account_id,
opp.account_name,
opp.opp_id,
opp.opp_name,
opp.start_dte_month,
opp.start_dte,
opp.end_dte_month,
opp.end_dte,
opp.mrr,
opp.mrr_change,
opp.is_active,
opp.mrr_acct,
CASE WHEN opp.mrr_change = 0 and opp.opp_category = 'recurring' then 0 else opp.mrr_change_acct end as mrr_change_acct,
opp_category,
opp.customer_category,
opp.revenue_category,
o.opp_owner_id,
o.opportunity_owner,
o.owner_description,
o.opp_closed_won_dte
from opps as opp
left join owner as o on (opp.opp_id = o.opp_id)
where opp.date_month < '2023-03-01'
and opp.date_month >= '2022-03-01'
order by opp.date_month asc, opp.opp_id 
),

/* Assigning Sales teams */
fct_opp_owner_teams as (
select distinct 
date_month,
account_id,
account_name,
opp_id,
opp_name,
start_dte_month,
start_dte,
end_dte_month,
end_dte,
mrr,
mrr_change,
is_active,
mrr_acct,
mrr_change_acct,
opp_category,
customer_category,
revenue_category,
opp_closed_won_dte,
opp_owner_id,
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
--CASE WHEN opp_closed_won_dte < start_dte then opp_closed_won_dte else start_dte end as close_dte
start_dte as close_dte
from fct_opp_owner 
order by opp_id, date_month asc
)

select * from fct_opp_owner_teams