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
opp_id,
opp_name,
mrr,
mrr_acct,
mrr_change_acct,
customer_category,
revenue_category,
first_active_month
from {{ref('fct_arr_opp')}}
where to_date(date_month) <= to_date(date_trunc('month',current_date()))
order by account_id, opp_id, date_month asc
),

account_region_lu as (
select distinct
account_id,
sales_region
from {{ref('account_sales_region_lu')}}
),

closed_won_dates as (
select distinct
opp_id,
to_timestamp(closed_won_dte) as closed_won_dte 
from {{ref('arr_opp_history')}}
),

fct_closed_won_region as (
select distinct
rd.date_month,
rd.account_id,
rd.account_name,
rd.opp_id,
rd.opp_name,
rd.mrr,
rd.mrr_acct,
rd.mrr_change_acct,
rd.customer_category,
rd.revenue_category,
to_timestamp(rd.first_active_month) as first_active_month,
arl.sales_region,
cwd.closed_won_dte
from raw_data as rd
left join account_region_lu as arl on (rd.account_id = arl.account_id)
left join closed_won_dates as cwd on (rd.opp_id = cwd.opp_id)
)

select * from fct_closed_won_region