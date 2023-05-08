{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'RENEWALS'
)
}}

/* Pulling in transformed SFDC renewal dates*/
/* This model needs to be updated with each month's transformed end dates for upcoming renewals */
with transformed_opp_id as (
select * from {{ref('transformed_opps_for_open_negotiations')}}
),

/* pulling in opportunities up for renewal */
/* This is a dataset of all opportunities that have ever been up for renewal */
renewal_opps_int as (
select distinct
CASE WHEN faor.opp_id in (select opp_id from transformed_opp_id) then faor.end_dte_month_raw else faor.date_month end as date_month,
faor.account_id,
faor.account_name,
faor.opp_id,
faor.opp_name,
faor.end_dte_raw as end_dte
from {{ref('fct_arr_opp_renewals')}} as faor
where opp_id not in ('0061R00000r6eqOQAQ',
                     '0061R000010PVABQA4',
                     '0061R000013fHgQQAU',
                     '0061R000010tH9RQAU',
                     '0063600000iS5wbAAC',
                     '0061R00000tFLB3QAO',
                     '0061R00000zAlU8QAK',
                     '0061R00000zBqNRQA0',
                     '0061R000014xeQwQAI',
                     '0061R000014xzVaQAI',
                     '006Dm000002cdEUIAY',
                     '006Dm000002cdEUIAY')
and opp_category = 'churn'
order by end_dte asc
),

renewal_opps as (
select distinct 
roi.date_month,
roi.account_id,
roi.account_name,
roi.opp_id,
roi.opp_name,
roi.end_dte,
fc.qtr_end_dte
from renewal_opps_int as roi
left join "DEV"."MARTS"."FY_CALENDAR" as fc on (to_date(roi.date_month) = to_date(fc.dte))  
)

select * from renewal_opps