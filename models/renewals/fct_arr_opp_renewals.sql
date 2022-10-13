{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

/* Pulling in opportunity data from fct_opps to id upcoming renewals */
with fct_opps as (
select distinct
to_timestamp(date_month) as date_month,
account_id,
account_name,
opp_id,
opp_name,
to_timestamp(start_dte_month) as start_dte_month,
to_timestamp(start_dte) as start_dte,
to_timestamp(end_dte_month) as end_dte_month,
to_timestamp(end_dte) as end_dte,
mrr,
mrr_change,
CASE WHEN mrr = 0 then mrr_change else mrr end as mrr_reporting,
is_active,
first_active_month,
last_active_month,
is_first_month,
is_last_month,
mrr_acct as mrr_acct,
mrr_change_acct,
CASE WHEN mrr_acct = 0 then mrr_change_acct else mrr_acct end as mrr_reporting_acct,
is_active_acct,
first_active_month_acct,
last_active_month_acct,
is_first_month_acct,
is_last_month_acct,
opp_category,
customer_category,
revenue_category
from {{ref('fct_arr_opp')}}
order by account_id, start_dte_month asc, date_month asc
),

/* Pulling in transformed SFDC data from arr_opp_history_transform model*/
/* This is because we need both transformed and non-transformed end dates */
/* non-transformed end dates for current month and previous month actual end dates */
raw_data_transformed as (
select * from {{ref('arr_opp_history_transformed')}}
),

/* Identifying all the opps with transformations to their end dates where we actually care about their raw (non-transfofrmed) contract end dates */
transformed_opp_id as (
select distinct
opp_id 
from raw_data_transformed
where end_dte_raw != end_dte
),

/* Subscription period intermediate view */
/* each row is an opportunity with transformed start and end dates */
/* one last adjustment to end months to account for opportunities that start on the 1st of a month and end on the last day of the month */
end_dtes_unadjusted as (
select 
opp_id,
CASE WHEN opp_id in (select * from transformed_opp_id) then end_dte_raw 
     else end_dte end as end_dte_raw,
CASE WHEN opp_id in (select * from transformed_opp_id) AND (end_dte_raw = last_day(end_dte_raw_month) and start_dte = start_dte_month) then dateadd(month,1,end_dte_raw_month)  
     WHEN opp_id in (select * from transformed_opp_id) then end_dte_raw_month
     WHEN opp_id not in (select * from transformed_opp_id) AND (end_dte = last_day(end_dte_month) and start_dte = start_dte_month) then dateadd(month,1,end_dte_month)  
     else end_dte_month end as end_dte_month_raw
from raw_data_transformed
),

/* Merging adjusted end dates with unadjusted end dates to get true end dates for renwals */
fct_renewals as (
select distinct
fo.date_month,
fo.account_id,
fo.account_name,
fo.opp_id,
fo.opp_name,
fo.start_dte_month,
fo.start_dte,
fo.end_dte_month,
edu.end_dte_month_raw,
fo.end_dte,
edu.end_dte_raw,
fo.mrr,
fo.mrr_change,
fo.mrr_reporting,
fo.is_active,
fo.first_active_month,
fo.last_active_month,
fo.is_first_month,
fo.is_last_month,
fo.mrr_acct,
fo.mrr_change_acct,
fo.mrr_reporting_acct,
fo.is_active_acct,
fo.first_active_month_acct,
fo.last_active_month_acct,
fo.is_first_month_acct,
fo.is_last_month_acct,
fo.opp_category,
fo.customer_category,
fo.revenue_category
from fct_opps as fo
left join end_dtes_unadjusted as edu on (fo.opp_id = edu.opp_id)
order by account_id, start_dte_month asc, date_month  
)

select * from fct_renewals

