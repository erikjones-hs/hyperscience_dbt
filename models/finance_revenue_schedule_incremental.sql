{{ config
(
    materialized ='incremental',
    database = 'PROD',
    schema = 'FINANCE'
)
}}

with rev_schedule_w_orders as (
select distinct  
rt.blng_account_c as account_id,
acct.name as account_name, 
opp.id as opp_id,
opp.name as opp_name,
opp.go_live_goal_date_c as go_live_date, 
opp.actual_go_live_date_c as actual_go_live_date, 
rs.name as rev_schedule_name, 
rt.name as rev_transaction_name,
rs.sr_revenue_account_c as revenue_account,
rt.blng_status_c as billing_status,
fp.blng_period_start_date_c as period_start_dte,
fp.blng_period_end_date_c as period_end_dte,
rt.blng_revenue_amount_c as revenue_amount,
to_date(current_date()) as date_ran
from "FIVETRAN_DATABASE"."SALESFORCE"."BLNG_REVENUE_TRANSACTION_C" as rt
left join "FIVETRAN_DATABASE"."SALESFORCE"."BLNG_REVENUE_SCHEDULE_C" as rs on (rt.blng_revenue_schedule_c = rs.id)
left join "FIVETRAN_DATABASE"."SALESFORCE"."OPPORTUNITY" as opp on (rs.sr_opportunity_c = opp.id)
left join "FIVETRAN_DATABASE"."SALESFORCE"."BLNG_FINANCE_PERIOD_C" as fp on (rt.blng_revenue_finance_period_c = fp.id)
left join "FIVETRAN_DATABASE"."SALESFORCE"."ACCOUNT" as acct on (rt.blng_account_c = acct.id)
where rs.is_active_c = true
and opp.is_deleted = false
and rt.is_deleted = false
and opp_id not in ('0061R00000kRNPDQA4') 
order by account_name, opp_name, rev_schedule_name asc, period_start_dte desc
)

select * from rev_schedule_w_orders;

{% if is_incremental() %}

  where date_ran >= (select max(date_ran) from {{ this }})

{% endif %}