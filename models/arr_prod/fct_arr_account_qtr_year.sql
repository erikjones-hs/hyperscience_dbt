{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with agg_account_arr as (
select *
from {{ ref('fct_arr_account') }}
where to_date(date_month) <= date_trunc(month,to_date(current_date()))
order by date_month asc
),

acct_meta_data as (
select * 
from {{ ref('sfdc_acct_meta_data') }}
),

deal_type as (
select * 
from {{ ref('dim_deal_type') }} 
),

fy_dates as (
select distinct 
dte,
CASE WHEN month in (1,2) then dateadd('year',-1,date_trunc(year,dte)) ELSE date_trunc(year,dte) end as fy_year,
fy_qtr_year,
qtr_end_dte
from "DEV"."MARTS"."FY_CALENDAR"
),

agg_account_dates as (
select distinct
aaa.date_month,
aaa.account_id,
aaa.account_name,
aaa.mrr_acct,
aaa.mrr_change_acct,
aaa.mrr_reporting_acct,
aaa.is_active_acct,
aaa.first_active_month,
aaa.last_active_month,
aaa.is_first_month_acct,
aaa.is_last_month_acct,
aaa.customer_category,
aaa.revenue_category,
aaa.months_since_start,
fd.fy_year,
fd.fy_qtr_year,
fd.qtr_end_dte 
from agg_account_arr as aaa
left join fy_dates as fd on (to_date(aaa.date_month) = to_date(date_trunc('month',fd.dte)))
order by aaa.account_id, aaa.date_month asc
),

agg_account_arr_qtr_year as (
select distinct
aaa.date_month,
aaa.account_id,
aaa.account_name,
aaa.mrr_acct,
aaa.mrr_change_acct,
aaa.mrr_reporting_acct,
aaa.is_active_acct,
aaa.first_active_month,
aaa.last_active_month,
aaa.is_first_month_acct,
aaa.is_last_month_acct,
aaa.customer_category,
aaa.revenue_category,
aaa.months_since_start,
aaa.fy_year,
aaa.fy_qtr_year,
aaa.qtr_end_dte,
amd.industry,
amd.billing_country_adjusted as billing_country,
amd.region,
amd.annual_revenue,
amd.revenue_range,
dt.deal_type,
row_number() over (partition by aaa.account_id, aaa.fy_year order by aaa.date_month desc) as fy_row_num,
row_number() over (partition by aaa.account_id, aaa.fy_qtr_year order by aaa.date_month desc) as fq_row_num  
from agg_account_dates as aaa
left join acct_meta_data as amd on (aaa.account_id = amd.account_id)
left join deal_type as dt on (aaa.account_id = dt.account_id)
order by account_id, date_month asc
)

select * from agg_account_arr_qtr_year