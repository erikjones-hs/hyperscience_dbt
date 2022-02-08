{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with raw_data as(
select distinct
date_month,
account_id,
account_name,
mrr_acct,
mrr_change_acct,
mrr_reporting_acct,
first_active_month,
last_active_month,
customer_category,
revenue_category,
months_since_start
from "DEV"."ERIKJONES"."MONTHLY_KPIS_FINANCE_ARR_ACCT"
where to_date(date_month) < to_date(date_trunc('month',current_date()))
order by account_id, date_month asc
),

fy_dates as (
select distinct 
dte,
CASE WHEN month in (1,2) then dateadd('year',-1,date_trunc(year,dte)) ELSE date_trunc(year,dte) end as fy_year,
fy_qtr_year,
qtr_end_dte
from "DEV"."MARTS"."FY_CALENDAR"
),

raw_dates as (
select distinct 
rd.date_month,
rd.account_id,
rd.account_name,
rd.mrr_acct,
rd.mrr_change_acct,
rd.mrr_reporting_acct,
rd.first_active_month,
rd.last_active_month,
rd.customer_category,
rd.revenue_category,
rd.months_since_start,
fd.fy_year,
fd.fy_qtr_year,
fd.qtr_end_dte
from raw_data as rd
left join fy_dates as fd on (to_date(rd.date_month) = to_date(date_trunc('month',fd.dte)))
),

rollup_retention_current as (
select 
date_month,
sum(mrr_acct) as total_arr_current
from raw_dates
where months_since_start >= 12
group by date_month
order by date_month asc
),

rollup_retention_past as (
select 
date_month,
qtr_end_dte,
sum(mrr_acct) as total_arr_past
from raw_dates
where months_since_start < 12
group by date_month, qtr_end_dte
order by date_month asc
),

rollup_retention as (
select * from (
select distinct
rrp.date_month,
rrp.qtr_end_dte,
rrp.total_arr_past,
rrc.total_arr_current,
lag(total_arr_past,12) over (order by rrp.date_month asc) as total_arr_12_months_ago,
(total_arr_current / total_arr_12_months_ago) as traditional_net_dollar_retention,
row_number() over (partition by qtr_end_dte order by rrp.date_month desc) as row_num
from rollup_retention_past as rrp
left join rollup_retention_current as rrc on (rrp.date_month = rrc.date_month)
order by rrp.date_month asc)
--where row_num = 1
),

qtr_rollup as (
select
qtr_end_dte,
sum(case when revenue_category = 'new' then mrr_change_acct else 0 end) as new_arr,
sum(case when revenue_category = 'expansion' then mrr_change_acct else 0 end) as expansion_arr,
sum(case when revenue_category = 'churn' then mrr_change_acct else 0 end) as churn_arr,
sum(case when customer_category = 'new' then 1 else 0 end) as new_customer,
sum(case when customer_category = 'churn' then 1 else 0 end) as churn_customer
from raw_dates
group by qtr_end_dte
order by qtr_end_dte asc
),

growth_acct_int as (
select distinct
qtr_end_dte,
new_arr,
expansion_arr,
churn_arr,
(new_arr + expansion_arr + churn_arr) as arr_change,
sum(arr_change) over (order by qtr_end_dte asc) as arr_running_total,
new_customer,
churn_customer,
(new_customer - churn_customer) as customer_change,
sum(customer_change) over (order by qtr_end_dte asc) as customer_running_total
from qtr_rollup 
order by qtr_end_dte asc
),

growth_acct as (
select distinct 
qtr_end_dte,
CASE WHEN to_date(qtr_end_dte) = '2018-05-31' then new_arr else lag(arr_running_total,1,0) over (order by qtr_end_dte asc) end as beginning_arr,
new_arr,
expansion_arr,
churn_arr,
arr_change,
CASE WHEN to_date(qtr_end_dte) = '2018-05-31' then arr_change else (beginning_arr + arr_change) end as ending_arr,
CASE WHEN to_date(qtr_end_dte) = '2018-05-31' then new_customer else lag(customer_running_total,1,0) over (order by qtr_end_dte asc) end as beginning_customer,
new_customer,
churn_customer,
CASE WHEN to_date(qtr_end_dte) = '2018-05-31' then customer_change else (beginning_customer + customer_change) end as ending_customer
from growth_acct_int
order by qtr_end_dte asc
),

growth_acct_with_metrics as (
select distinct
ga.qtr_end_dte,
ga.beginning_arr,
ga.new_arr,
ga.expansion_arr,
ga.churn_arr,
ga.ending_arr,
ga.beginning_customer,
ga.new_customer,
ga.churn_customer,
ga.ending_customer,
(ga.ending_arr / NULLIFZERO(ga.ending_customer)) as arr_per_customer,
(ga.ending_arr - lag(ga.ending_arr,1,0) over (order by ga.qtr_end_dte asc)) / NULLIFZERO(lag(ga.ending_arr,1,0) over (order by ga.qtr_end_dte asc)) as arr_growth_qoq,
(ga.new_arr / NULLIFZERO(ga.beginning_arr)) as new_arr_percent_beg_arr,
(ga.churn_arr / NULLIFZERO(ga.beginning_arr)) as churn_arr_percent_beg_arr,
(ga.new_arr / NULLIFZERO(ga.new_customer)) as new_arr_per_new_customers
from growth_acct as ga   
order by ga.qtr_end_dte asc
)

select * from growth_acct_with_metrics; 