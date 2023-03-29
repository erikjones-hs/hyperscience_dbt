{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with agg_account_arr_int as (
    
select distinct
to_timestamp(date_month) as date_month,
account_id,
account_name,
mrr_acct,
mrr_change_acct,
mrr_reporting_acct,
is_active_acct,
to_timestamp(first_active_month_acct) as first_active_month,
to_timestamp(last_active_month_acct) as last_active_month,
is_first_month_acct,
is_last_month_acct,
customer_category,
revenue_category,
datediff(month,first_active_month_acct,date_month) as months_since_start
from {{ ref('fct_arr_opp') }}
order by date_month asc
),

fy_dates as (
select distinct 
dte,
CASE WHEN month in (1,2) then dateadd('year',-1,date_trunc(year,dte)) ELSE date_trunc(year,dte) end as fy_year,
fy_qtr_year,
qtr_end_dte
from "DEV"."MARTS"."FY_CALENDAR"
),

agg_account_arr_int2 as (
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
from agg_account_arr_int as aaa
left join fy_dates as fd on (to_date(aaa.date_month) = to_date(date_trunc('month',fd.dte)))
order by aaa.account_id, aaa.date_month asc
),

agg_account_arr as (
select distinct
date_month,
account_id,
account_name,
mrr_acct,
mrr_change_acct,
mrr_reporting_acct,
is_active_acct,
first_active_month,
last_active_month,
is_first_month_acct,
is_last_month_acct,
customer_category,
revenue_category,
months_since_start,
fy_year,
fy_qtr_year,
qtr_end_dte,
row_number() over (partition by account_id, fy_year order by date_month desc) as fy_row_num,
row_number() over (partition by account_id, fy_qtr_year order by date_month desc) as fq_row_num  
from agg_account_arr_int2
order by account_id, date_month asc
)

select * from agg_account_arr
