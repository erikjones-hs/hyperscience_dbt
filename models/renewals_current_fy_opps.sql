{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'RENEWALS'
)
}}

with renewal_opps as (
select distinct
date_month,
account_id,
account_name,
opp_id,
opp_name,
mrr_change,
end_dte
from {{ ref('fct_arr_opp_renewals') }}
where opp_category = 'churn'
and to_date(date_month) >= date_trunc('month',to_date(current_date()))
and to_date(date_month) <= '2024-02-01'
and opp_id not in ('0061R000010usoKQAQ','0061R000010ujZ5QAI')
order by date_month asc
),

open_opps_int as (
select distinct
opp_id,
opp_name,
account_id,
account_name,
opp_arr,
opp_net_new_arr,
opp_stage_name,
opp_commit_status,
CASE WHEN opp_id = '0061R00000yGqH3QAK' then '0061R000014wIeUQAU' else prior_opp_id end as prior_opp_id,
opp_close_dte
from "DEV"."SALES"."SALESFORCE_AGG_OPPORTUNITY"
where opp_stage_name not in ('Closed Won','Opp DQed')
and is_deleted = false
),

open_opps as (
select * 
from open_opps_int
where prior_opp_id in (select distinct opp_id from renewal_opps)  
and opp_id not in ('0061R000014wNrwQAE')
),

combined_opps as (
select distinct
ro.date_month as renewal_month,
ro.account_id,
ro.account_name,
ro.opp_id as existing_opp_id,
ro.opp_name as existing_opp_name,
ro.mrr_change as potential_churn_amount,
ro.end_dte as existing_opp_renewal_date,
oo.opp_id as open_opp_id,
oo.opp_name as open_opp_name,
oo.opp_arr as open_opp_arr,
oo.opp_net_new_arr as open_opp_net_new_arr_raw,
oo.opp_close_dte as open_opp_close_dte,
oo.opp_stage_name,
oo.opp_commit_status as open_opp_commit_status,
CASE WHEN open_opp_id IS NULL then potential_churn_amount else (potential_churn_amount + open_opp_arr) end as net_new_arr,
CASE WHEN net_new_arr > 0 then 'expansion'
     WHEN net_new_arr < 0 then 'churn'
     WHEN net_new_arr = 0 then 'flat'
     ELSE 'other' end as renewal_type
from renewal_opps as ro
left join open_opps as oo on (ro.opp_id = oo.prior_opp_id)
order by date_month asc
)

select * from combined_opps