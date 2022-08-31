{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with mqls as (
select
lclcss.date as mql_dte,
lclcss.person_id,
slac.lead_source,
slac.email,
slac.lead_type,
slac.secondary_lead_source,
slac.source_first_lead_source_detail,
slac.last_lead_source,
CASE WHEN slac.email IS NOT NULL THEN split_part(slac.email,'@',2) ELSE NULL end as email_domain
from {{ref('lead_contact_life_cycle_status_changes')}} as lclcss
left join {{ref('salesforce_leads_and_contacts')}} as slac 
    on (lclcss.person_id = slac.person_id)
where lclcss.status_change in ('MQL from SAL','MQL')
and slac.lead_type = 'Sales'
and (email_domain IS NULL OR email_domain not in ('hyperscience.com'))
),

fy_dates as (
select distinct
dte,
month,
day_of_year,
day_of_qtr,
fy_quarter,
fy_year
from "DEV"."MARTS"."FY_CALENDAR"
where to_date(dte) <= to_date(current_date()) 
),

fct_mqls as (
select distinct
mql_dte,
person_id,
lead_source,
email,
lead_type,
secondary_lead_source,
source_first_lead_source_detail,
last_lead_source,
fy.fy_year,
fy.dte
from fy_dates as fy
left join mqls on (to_date(mqls.mql_dte) = to_date(fy.dte))
where fy.dte >= '2018-01-01'
order by fy.dte asc
),

fct_mql_agg as (
select 
fy_year,
dte,
count(distinct person_id) as num_leads
from fct_mqls
group by dte, fy_year
order by dte asc
),

fy_agg as (
select 
fy_year,
to_timestamp(dte) as dte,
sum(num_leads) over (partition by fy_year order by dte asc rows between unbounded preceding and current row) as num_mqls
from fct_mql_agg 
where to_date(dte) <= to_date(current_date())
order by fy_year asc, dte asc
)

select * from fy_agg