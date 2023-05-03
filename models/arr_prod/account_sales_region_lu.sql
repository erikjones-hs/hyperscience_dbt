{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

/* Need to pull in opportunity owner to assign to sales team */
with owner as (
select distinct
aoi.opp_id,
aoi.opp_name,
aoi.account_id,
aoi.account_name,
aoi.opp_owner_id,
aoi.opportunity_owner,
aoi.owner_description,
u.department
from {{ ref('agg_opportunity_incremental') }} as aoi
left join "FIVETRAN_DATABASE"."SALESFORCE"."USER" as u on (u.id = aoi.opp_owner_id)
where to_date(date_ran) = dateadd(day,-1,(to_date(current_date)))
),

active_opps as (
select distinct
opp_id,
account_id,
account_name,
mrr
from {{ ref('fct_arr_opp') }}
where is_active = TRUE
),

active_opp_acct_owner as (
select distinct
ao.opp_id,
ao.account_id,
ao.account_name,
CASE WHEN ao.account_id = '0011R00002dAkDPQA0' then 'EMEA'
     WHEN ao.account_id = '0013600001ILwjXAAT' then 'NA'
     when ao.account_id = '00136000015Fz7iAAC' then 'APJ' 
     ELSE o.department end as department 
from active_opps as ao 
left join owner as o on (ao.opp_id = o.opp_id)
),

account_department as (
select distinct
account_id,
account_name,
CASE WHEN (department in ('Central','Federal','East','Channel','Ops','Customer Success','West','NA') or department IS NULL) then 'NA'
     WHEN department in ('EMEA') then 'EMEA'
     WHEN department in ('APJ','BPO') then 'APAC'
     ELSE 'OTHER' end as sales_region
from active_opp_acct_owner
order by sales_region  
)

select account_id, sales_region from account_department