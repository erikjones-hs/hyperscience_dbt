{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with mqls as (
select distinct 
mql_dte,
person_id
from "DEV"."ERIKJONES"."MONTHLY_KPIS_MARKETING_MQLS"
order by mql_dte asc
),

fy_dates as (
select distinct 
dte,
CASE WHEN month in (1,2) then dateadd('year',-1,date_trunc(year,dte)) ELSE date_trunc(year,dte) end as fy_year,
fy_qtr_year,
qtr_end_dte
from "DEV"."MARTS"."FY_CALENDAR"
),

fy_mqls as (
select distinct
to_timestamp(m.mql_dte) as mql_dte,
m.person_id,
fd.fy_year,
fd.fy_qtr_year,
fd.qtr_end_dte,
to_timestamp(fd.dte) as dte
from mqls as m 
right join fy_dates as fd on (m.mql_dte = fd.dte)
where dte >= '2018-01-01'
order by dte asc
),

mql_daily_agg as (
select distinct 
date_trunc('month',mql_dte) as dte_month,
fy_year,
fy_qtr_year,
qtr_end_dte,
count(distinct person_id) as num_leads
from fy_mqls
where person_id IS NOT NULL
group by dte_month, fy_year, fy_qtr_year, qtr_end_dte
order by dte_month asc
),

fy_mqls_agg as (
select 
fy_year,
dte_month,
sum(num_leads) over (partition by fy_year order by dte_month asc rows between unbounded preceding and current row) as num_leads
from mql_daily_agg
where to_date(dte_month) <= to_date(date_trunc('month',current_date()))
order by fy_year asc, dte_month asc
)

select * from fy_mqls_agg