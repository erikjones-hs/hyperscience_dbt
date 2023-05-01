{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with capacity_raw as (
select distinct
last_day(to_date(date)) as dte,
total_sales_capacity
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
where to_date(dte) >= '2022-03-01'
order by dte asc
),

fy_dates as (
select distinct 
dte,
CASE WHEN month in (1,2) then dateadd('year',-1,date_trunc(year,dte)) ELSE date_trunc(year,dte) end as fy_year,
fy_qtr_year,
fy_quarter,
qtr_end_dte
from "DEV"."MARTS"."FY_CALENDAR"
),

arr_budget as (
select distinct
last_day(to_date(date)) as dte,
new_arr_budget
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
where to_date(dte) >= '2022-03-01'
order by dte asc
),

/* Combining Forecast Data with FY QTR End Dates */
/* Because we need to cum sum over each FY QTR to get correct numbers */
budget_int as (
select distinct
ab.dte as month_end_dte,
ab.new_arr_budget as new_arr,
fd.qtr_end_dte
from arr_budget as ab
left join fy_dates as fd on (to_date(ab.dte) = to_date(fd.dte))
order by ab.dte asc
),

/* Aggregatig by fiscal quarter */
new_arr_qtr_agg as (
select distinct
qtr_end_dte,
sum(new_arr) as new_arr_budget
from budget_int
group by qtr_end_dte
order by qtr_end_dte asc
),

/* Combining Capacity Data with FY QTR End Dates */
/* Because we need to cum sum over each FY QTR to get correct numbers */
capacity_int as (
select distinct
cr.dte as month_end_dte,
cr.total_sales_capacity as sales_capacity,
fd.qtr_end_dte
from capacity_raw as cr
left join fy_dates as fd on (to_date(cr.dte) = to_date(fd.dte))
order by cr.dte asc
),

/* Aggregatig Capacity by fiscal quarter */
capacity_qtr_agg as (
select distinct
qtr_end_dte as dte,
sum(sales_capacity) as total_capacity
from capacity_int
group by qtr_end_dte
order by qtr_end_dte asc
),

fct_pipeline as (
select distinct
to_timestamp(opp.date_ran) as date_ran,
opp.opp_id,
opp.opp_name,
opp.account_name,
opp.account_industry,
opp.account_sales_region,
opp.partner_account_name,
opp.opp_stage_name,
opp.opp_lead_source,
opp.opp_is_marketing_influenced_flag,
opp.opp_close_dte,
opp.opp_arr,
opp.opp_net_new_arr, 
opp.opportunity_owner,
opp.owner_description,
opp.opp_pipeline_category,
opp.opp_revenue_type,
fy.dte,
fy.fy_quarter,
fy.fy_year
from {{ ref('new_business_pipeline_current') }} as opp
right join fy_dates as fy on (to_date(opp_close_dte) = to_date(fy.dte))
where fy.dte >= '2022-03-01'
order by fy.dte asc
),

pipeline_agg_int as (
select distinct
dte,
fy_quarter,
opp_pipeline_category,
sum(opp_net_new_arr) as opp_net_new_arr
from fct_pipeline
group by dte, fy_quarter, opp_pipeline_category
order by dte asc, fy_quarter, opp_pipeline_category
),

pipeline_agg as (
select 
fy_quarter,
dte as close_date,
opp_pipeline_category,
ZEROIFNULL(sum(opp_net_new_arr) over (partition by fy_quarter, opp_pipeline_category order by dte asc rows between unbounded preceding and current row)) as cum_sum_net_new_arr
from pipeline_agg_int
order by fy_quarter asc, dte asc
),

fct_capacity_coverage as (
select distinct
to_timestamp(cqa.dte) as dte,
cqa.total_capacity,
naqa.new_arr_budget,
pa.opp_pipeline_category,
pa.cum_sum_net_new_arr
from capacity_qtr_agg as cqa 
left join new_arr_qtr_agg as naqa on (cqa.dte = naqa.qtr_end_dte)
left join pipeline_agg as pa on (cqa.dte = pa.close_date)
order by dte asc
)

select * from fct_capacity_coverage