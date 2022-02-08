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
is_active_acct,
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
rd.is_active_acct,
rd.customer_category,
rd.revenue_category,
rd.months_since_start,
fd.fy_year,
fd.fy_qtr_year,
fd.qtr_end_dte
from raw_data as rd
left join fy_dates as fd on (to_date(rd.date_month) = to_date(date_trunc('month',fd.dte)))
order by account_id, date_month asc
),

first_active_qtr_rev as (
select distinct
qtr_end_dte as first_active_quarter,
account_id,
account_name,
mrr_acct
from raw_dates
where revenue_category = 'new'
order by qtr_end_dte asc
),

rev_first_active_qtr as (
select distinct
first_active_quarter,
sum(mrr_acct) as cohort_rev_amt
from first_active_qtr_rev
group by first_active_quarter
order by first_active_quarter
),

active_rev_quarters as (
select * from (
select distinct
date_month,
qtr_end_dte,
account_id,
account_name,
mrr_acct,
revenue_category,
row_number() over (partition by account_id, qtr_end_dte order by date_month desc) as row_num
from raw_dates
order by account_id, date_month asc
)
where row_num = 1
order by account_id, date_month asc
),

first_active_rev_quarters as (
select distinct
arq.date_month,
arq.qtr_end_dte,
arq.account_id,
arq.account_name,
faqr.mrr_acct as initial_arr,
arq.mrr_acct,
CASE WHEN initial_arr = arq.mrr_acct then initial_arr 
     WHEN initial_arr < arq.mrr_acct then initial_arr
     WHEN initial_arr > arq.mrr_acct then arq.mrr_acct
     ELSE initial_arr end as arr,
faqr.first_active_quarter,
(datediff(month,faqr.first_active_quarter,arq.qtr_end_dte) / 3) + 1 as num_quarters_since_start
from active_rev_quarters as arq 
left join first_active_qtr_rev as faqr on (arq.account_id = faqr.account_id)
order by account_id, date_month asc 
),

fct_rev_retention_cohort_int as (
select 
first_active_quarter,
num_quarters_since_start,
sum(arr) as tot_rev_amt
from first_active_rev_quarters
group by first_active_quarter, num_quarters_since_start
order by first_active_quarter, num_quarters_since_start
),

fct_gross_rev_retention_cohort as (
select distinct
frrci.first_active_quarter as first_active_quarter,
frrci.num_quarters_since_start,
frrci.tot_rev_amt,
rfaq.cohort_rev_amt,
(frrci.tot_rev_amt / NULLIFZERO(rfaq.cohort_rev_amt)) as gross_rev_retention 
from fct_rev_retention_cohort_int as frrci
left join rev_first_active_qtr as rfaq on (frrci.first_active_quarter = rfaq.first_active_quarter) 
order by first_active_quarter asc, num_quarters_since_start asc
)

select * from fct_gross_rev_retention_cohort