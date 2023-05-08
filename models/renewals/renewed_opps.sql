{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'RENEWALS'
)
}}

with active_opps_int as (
select distinct
date_month,
opp_id,
opp_name,
account_id,
account_name,
mrr_change_acct,
customer_category
from {{ ref('fct_arr_opp') }}
where is_active = TRUE
and date_month <= date_trunc(month,to_date(current_date))
),

prior_opp_ids as (
select distinct
opp_id,
opp_name,
CASE WHEN opp_id = '0061R00000yGqH3QAK' then '0061R000014wIeUQAU' 
     WHEN opp_id = '0061R00001A6iWpQAJ' then '0061R000016kGCyQAM'
     when opp_id = '0061R000014wNrVQAU' then '0061R00000yFci4QAC'
     when opp_id = '0061R00001BAugdQAD' then '0061R00000yFonNQAS'
     when opp_id = '0061R000013f0rkQAA' then '0061R00000zBqNRQA0'
     when opp_id = '0061R00000zAlU8QAK' then '0061R00000oCWm8QAG'
     when opp_id = '006Dm000002dhpbIAA' then '0061R00000r6r1iQAA'
     when opp_id = '0061R00001A6BeZQAV' then '0061R000014wI4qQAE'
     when opp_id = '0061R00001BAvduQAD' then '0061R00000uINyXQAW'
     when opp_id = '0061R00000oE2hbQAC' then NULL
     else prior_opp_id end as prior_opp_id
from "DEV"."SALES"."SALESFORCE_AGG_OPPORTUNITY"
where is_deleted = false
),

active_opps as (
select distinct
aoi.date_month,
aoi.opp_id,
aoi.opp_name,
aoi.account_id,
aoi.account_name,
aoi.mrr_change_acct,
aoi.customer_category,
poi.prior_opp_id
from active_opps_int as aoi 
left join prior_opp_ids as poi on (aoi.opp_id = poi.opp_id)
),

revenue_type as (
select distinct
opp_id,
opp_revenue_type
from {{ ref('agg_opportunity_incremental') }}
where date_ran = dateadd(day,-1,(to_date(current_date)))
),

opp_type as (
select distinct
ao.date_month,
ao.account_id,
ao.account_name,
ao.opp_id,
ao.opp_name,
ao.mrr_change_acct,
ao.customer_category,
ao.prior_opp_id,
CASE WHEN rt.opp_revenue_type in ('Renewal','Renewal w/ Upsell') then 'Renewal' else rt.opp_revenue_type end as opp_type,
row_number() over (partition by ao.account_id, ao.opp_id, opp_type order by ao.date_month asc) as opp_type_flag
from active_opps as ao 
left join revenue_type as rt on (ao.opp_id = rt.opp_id)
order by account_id, date_month asc
),

renewals as (
select distinct
date_month,
account_id,
account_name,
opp_id,
opp_name,
opp_type,
prior_opp_id,
CASE WHEN opp_type = 'Renewal' and opp_type_flag = 1 and customer_category = 'active' then 1 else 0 end as renewal_month,
max(renewal_month) over (partition by account_id) as has_renewed,
sum(renewal_month) over (partition by account_id) as num_renewals,
CASE WHEN opp_id = '0061R000010tH9RQAU' then 0 else mrr_change_acct end as renewal_arr_change
from opp_type
where renewal_month = 1
and to_date(date_month) <= dateadd(month,1,date_trunc(month,to_date(current_date())))
order by account_id, date_month asc
)

select * from renewals