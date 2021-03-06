{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with agg_account_arr as (
    
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
)

select * from agg_account_arr
