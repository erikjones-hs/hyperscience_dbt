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
and u.is_active = TRUE
and aoi.opp_stage_name not in ('Closed Lost','Closed Won')
),

opp_acct_owner as (
select distinct
opp_id,
account_id,
account_name,
CASE WHEN account_id = '0011R00002dAkDPQA0' then 'EMEA'
     WHEN account_id = '0013600001ILwjXAAT' then 'NA'
     when account_id = '00136000015Fz7iAAC' then 'APJ' 
     ELSE department end as department 
from owner as o
),

account_department as (
select distinct
account_id,
account_name,
CASE WHEN (department in ('Central','Federal','East','Channel','Ops','Customer Success','West','NA') or department IS NULL) then 'NA'
     WHEN department in ('EMEA') then 'EMEA'
     WHEN department in ('APJ','BPO') then 'APAC'
     ELSE 'OTHER' end as sales_region
from opp_acct_owner
order by sales_region  
)

select account_id, sales_region from account_department