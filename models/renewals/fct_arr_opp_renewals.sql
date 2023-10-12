{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

/* Pulling in opportunity data from fct_opps to id upcoming renewals */
/* joining with arr opp history tabe to get untransformed end dates (end date raw) */
with fct_opps as (
select distinct
to_timestamp(fao.date_month) as date_month,
fao.account_id,
fao.account_name,
fao.opp_id,
fao.opp_name,
to_timestamp(fao.start_dte_month) as start_dte_month,
to_timestamp(fao.start_dte) as start_dte,
to_timestamp(fao.end_dte_month) as end_dte_month,
CASE WHEN aoh.opp_id = '0061R00001A4pwYQAR' then to_timestamp('2023-10-01') 
     WHEN aoh.opp_id = '0061R00001A4pwsQAB' then to_timestamp('2023-10-01')
     else to_timestamp(date_trunc('month',aoh.end_dte_raw)) end as end_dte_raw_month,
to_timestamp(fao.end_dte) as end_dte,
CASE WHEN aoh.opp_id = '0061R00001A4pwYQAR' then to_timestamp('2023-10-29') 
     WHEN aoh.opp_id = '0061R00001A4pwsQAB' then to_timestamp('2023-10-29')
     else to_timestamp(aoh.end_dte_raw) end as end_dte_raw,
fao.mrr,
fao.mrr_change,
CASE WHEN fao.mrr = 0 then fao.mrr_change else fao.mrr end as mrr_reporting,
fao.is_active,
fao.first_active_month,
fao.last_active_month,
fao.is_first_month,
fao.is_last_month,
fao.mrr_acct as mrr_acct,
fao.mrr_change_acct,
CASE WHEN fao.mrr_acct = 0 then fao.mrr_change_acct else fao.mrr_acct end as mrr_reporting_acct,
fao.is_active_acct,
fao.first_active_month_acct,
fao.last_active_month_acct,
fao.is_first_month_acct,
fao.is_last_month_acct,
fao.opp_category,
fao.customer_category,
fao.revenue_category
from {{ref('fct_arr_opp')}} as fao
left join {{ref('arr_opp_history')}} as aoh on (fao.opp_id = aoh.opp_id)
order by account_id, start_dte_month asc, date_month asc
),

/* Pulling in transformed SFDC renewal dates*/
/* This model needs to be updated with each month's transformed end dates for upcoming renewals */
transformed_opp_id as (
select * from {{ref('transformed_opps_for_open_negotiations')}}
),

/* Subscription period intermediate view */
/* each row is an opportunity with transformed start and end dates */
/* one last adjustment to end months to account for opportunities that start on the 1st of a month and end on the last day of the month */
end_dtes_unadjusted as (
select 
opp_id,
CASE WHEN opp_id in (select * from transformed_opp_id) then end_dte_raw 
     WHEN opp_id in (select * from transformed_opp_id) AND (end_dte_raw = last_day(end_dte_raw_month) and start_dte = start_dte_month) then dateadd(day,2,end_dte_raw) 
     WHEN opp_id not in (select * from transformed_opp_id) AND (end_dte = last_day(end_dte_month) and start_dte = start_dte_month) then dateadd(day,2,end_dte)  
     else end_dte end as end_dte_raw,
CASE WHEN opp_id in (select * from transformed_opp_id) AND (end_dte_raw = last_day(end_dte_raw_month) and start_dte = start_dte_month) then dateadd(month,1,end_dte_raw_month)  
     WHEN opp_id in (select * from transformed_opp_id) then end_dte_raw_month
     WHEN opp_id not in (select * from transformed_opp_id) AND (end_dte = last_day(end_dte_month) and start_dte = start_dte_month) then dateadd(month,1,end_dte_month)  
     else end_dte_month end as end_dte_month_raw
from fct_opps
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