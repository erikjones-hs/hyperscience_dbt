{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'CUSTOMER_USAGE'
)
}}

with go_live_dates_int as (
select distinct 
dte,
account_id,
account_name,
opp_id,
opp_name,
go_live_date,
start_date as contract_start_date,
live_customer_fl,
ttv_days,
ttv_months
from {{ref('go_live_history')}}
),

go_live_months as (
select distinct
account_id,
date_trunc(month,to_date(dte)) as dte_month,
max(live_customer_fl) as live_customer_fl
from go_live_dates_int
where dte_month <= date_trunc(month, to_date(current_date()))
group by account_id, dte_month
order by account_id, dte_month asc
),

activation as (
select distinct
date_month,
account_id,
sfdc_account_name,
is_live_customer as activated_customer_fl,
first_active_month as activation_month,
last_active_month as last_month_usage_data_received
from {{ref('monthly_activated_customers_estimate')}}
),

usage_meta_data as (
select distinct
to_timestamp(glm.dte_month) as dte_month,
gld.account_id,
gld.account_name,
gld.contract_start_date,
gld.go_live_date,
gld.ttv_days,
gld.ttv_months,
a.activation_month,
a.last_month_usage_data_received,
CASE WHEN a.activated_customer_fl IS NULL THEN 0 else a.activated_customer_fl end as activated_customer_fl,
glm.live_customer_fl
from go_live_months as glm
left join go_live_dates_int as gld on (glm.account_id = gld.account_id)
left join activation as a on (gld.account_id = a.account_id AND to_date(glm.dte_month) = to_date(a.date_month))
order by gld.account_id, dte_month asc
)

select * from usage_meta_data