{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'CUSTOMER_USAGE'
)
}}

with forecast as (
select distinct
ds as dte,
customer as account_id,
yhat,
yhat_upper,
yhat_lower
from "DEV"."ERIKJONES"."USAGE_FORECAST_ALL"
order by customer, dte asc
),

actuals as (
select distinct
customer as account_id, 
to_date(dte_month) as dte, 
total_pages_created as total_pages
from {{ref('usage_all_customers')}}
where to_date(dte_month) <= '2022-11-01'
order by customer, dte asc
),

meta_data as (
select distinct 
account_id,
account_name,
opp_id,
opp_name,
start_dte,
end_dte,
contract_length_months,
arr,
is_opp_active_fl,
contract_pages_annual
from {{ref('acct_meta_data')}}
),

fct_forecast_actuals as (
select distinct
f.dte,
f.account_id,
md.account_name,
md.start_dte,
md.end_dte,
md.contract_length_months,
md.arr,
md.is_opp_active_fl,
md.contract_pages_annual,
f.yhat_lower,
a.total_pages,
f.yhat,
f.yhat_upper
from forecast as f 
left join actuals as a on (f.account_id = a.account_id AND to_date(f.dte) = to_date(a.dte))
left join meta_data as md on (f.account_id = md.account_id)
order by f.account_id, f.dte asc
)

select * from fct_forecast_actuals
