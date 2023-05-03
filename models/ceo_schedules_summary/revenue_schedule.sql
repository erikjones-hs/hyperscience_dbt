{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'SCHEDULES_SUMMARY'
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
from {{ref('fct_arr_account')}}
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

account_region_lu as (
select distinct
account_id,
sales_region
from {{ref('account_sales_region_lu')}}
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
arl.sales_region,
fd.fy_year,
fd.fy_qtr_year,
fd.qtr_end_dte
from raw_data as rd
left join fy_dates as fd on (to_date(rd.date_month) = to_date(date_trunc('month',fd.dte)))
left join account_region_lu as arl on (rd.account_id = arl.account_id) 
),

monthly_rollup as (
select
date_month,
sum(case when revenue_category = 'new' then mrr_change_acct else 0 end) as new_arr,
sum(case when revenue_category = 'expansion' then mrr_change_acct else 0 end) as expansion_arr,
sum(case when revenue_category = 'churn' then mrr_change_acct else 0 end) as churn_arr,
sum(case when revenue_category = 'churn' and sales_region = 'NA' then mrr_change_acct else 0 end) as na_churn,
sum(case when revenue_category = 'churn' and sales_region = 'EMEA' then mrr_change_acct else 0 end) as emea_churn,
sum(case when revenue_category = 'churn' and sales_region = 'APAC' then mrr_change_acct else 0 end) as apac_churn,
sum(case when revenue_category = 'de-book' then mrr_change_acct else 0 end) as de_book_arr, 
sum(case when revenue_category = 'new' and sales_region = 'NA' then mrr_change_acct else 0 end) as na_new_arr,
sum(case when revenue_category = 'expansion' and sales_region = 'NA' then mrr_change_acct else 0 end) as na_expansion_arr,
sum(case when revenue_category = 'new' and sales_region = 'EMEA' then mrr_change_acct else 0 end) as emea_new_arr,
sum(case when revenue_category = 'expansion' and sales_region = 'EMEA' then mrr_change_acct else 0 end) as emea_expansion_arr,
sum(case when revenue_category = 'new' and sales_region = 'APAC' then mrr_change_acct else 0 end) as apac_new_arr,
sum(case when revenue_category = 'expansion' and sales_region = 'APAC' then mrr_change_acct else 0 end) as apac_expansion_arr,
sum(case when customer_category = 'new' then 1 else 0 end) as new_customer,
sum(case when customer_category = 'churn' then 1 else 0 end) as churn_customer,
sum(case when customer_category = 'de-book' then 1 else 0 end) as de_book_customer
from raw_dates
group by date_month
order by date_month asc
),

growth_acct_int as (
select distinct
date_month,
new_arr,
expansion_arr,
(new_arr + expansion_arr) as new_bookings,
(na_new_arr + na_expansion_arr) as na_new_bookings,
(emea_new_arr + emea_expansion_arr) as emea_new_bookings,
(apac_new_arr + apac_expansion_arr) as apac_new_bookings,
churn_arr,
na_churn,
emea_churn,
apac_churn,
de_book_arr,
(new_arr + expansion_arr + churn_arr + de_book_arr) as arr_change,
sum(arr_change) over (order by date_month asc) as arr_running_total,
new_customer,
churn_customer,
de_book_customer,
(new_customer - churn_customer - de_book_customer) as customer_change,
sum(customer_change) over (order by date_month asc) as customer_running_total
from monthly_rollup 
order by date_month asc
),

growth_acct as (
select distinct 
date_month,
CASE WHEN to_date(date_month) = '2018-04-01' then new_arr else lag(arr_running_total,1,0) over (order by date_month asc) end as beginning_arr,
new_arr,
expansion_arr,
new_bookings,
na_new_bookings,
emea_new_bookings,
apac_new_bookings,
churn_arr,
na_churn,
emea_churn,
apac_churn,
de_book_arr,
arr_change,
CASE WHEN to_date(date_month) = '2018-04-01' then arr_change else (beginning_arr + arr_change) end as ending_arr,
CASE WHEN to_date(date_month) = '2018-04-01' then new_customer else lag(customer_running_total,1,0) over (order by date_month asc) end as beginning_customer,
new_customer,
churn_customer,
de_book_customer,
CASE WHEN to_date(date_month) = '2018-04-01' then customer_change else (beginning_customer + customer_change) end as ending_customer
from growth_acct_int
order by date_month asc
),

growth_acct_with_metrics as (
select distinct
ga.date_month,
ga.beginning_arr,
ga.new_arr,
ga.expansion_arr,
ga.new_bookings,
ga.na_new_bookings,
ga.emea_new_bookings,
ga.apac_new_bookings,
ga.churn_arr,
ga.na_churn,
ga.emea_churn,
ga.apac_churn,
ga.de_book_arr,
ga.ending_arr,
(ga.new_arr + ga.expansion_arr + ga.churn_arr + ga.de_book_arr) as net_new_arr,
ga.beginning_customer,
ga.new_customer,
ga.churn_customer,
ga.de_book_customer,
ga.ending_customer,
(ga.ending_arr / NULLIFZERO(ga.ending_customer)) as arr_per_customer,
(ga.ending_arr - lag(ga.ending_arr,1,0) over (order by ga.date_month asc)) / NULLIFZERO(lag(ga.ending_arr,1,0) over (order by ga.date_month asc)) as arr_growth_mom,
(ga.new_arr / NULLIFZERO(ga.beginning_arr)) as new_arr_percent_beg_arr,
(ga.churn_arr / NULLIFZERO(ga.beginning_arr)) as churn_arr_percent_beg_arr,
(ga.new_arr / NULLIFZERO(ga.new_customer)) as new_arr_per_new_customers
from growth_acct as ga   
order by ga.date_month asc
)

select * from growth_acct_with_metrics 