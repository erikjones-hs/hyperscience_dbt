{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'DATA_QC'
)
}}

with arr_customers_current as (
select distinct
account_id,
account_name,
first_active_month_acct,
mrr_acct as arr
from {{ref('fct_arr_opp')}}
where to_date(date_month) = date_trunc(month,to_date(current_date()))
and is_active = TRUE
),

customer_usage as (
select distinct
account_id,
sfdc_account_name
from {{ref('fct_usage')}}
),

fct_arr_usage as (
select distinct
acc.account_id,
acc.arr,
acc.first_active_month_acct,
datediff(month,acc.first_active_month_acct, date_trunc(month,to_date(current_date()))) as months_since_arr_start,
CASE WHEN acc.account_name = '8053580156557' then 'Department of Justice' else acc.account_name end as account_name,
CASE WHEN cu.account_id IS NOT NULL then 1 else 0 end as has_usage_data_fl
from arr_customers_current as acc
left join customer_usage as cu on (acc.account_id = cu.account_id)
order by account_name 
)

select * from fct_arr_usage







