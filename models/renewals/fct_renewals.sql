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

/* These are all the opps that have or will be ever up for renewal */
renewal_opps as (
select * from {{ref('opps_up_for_renewal')}} 
),

/* opportunities that have renewed */
/* This is a list of all closed won opps whose opp type = Renewal */
renewals as (
select * from {{ref('renewed_opps')}} 
),

/* This is a dataset of all potential renewals to date, and their outcomes */
renewals_with_outcomes as (
select * from {{ref('renewals_with_outcomes')}} 
),

/* Want to get true ARR up for renewal */
arr_opp_history as (
select distinct 
opp_id,
CASE WHEN opp_id = '0061R00001BAPkAQAX' then 330000 else round(opp_arr) end as arr
from {{ref('arr_opp_history')}}
),

/* opportunities that have churned */
churn as (
select distinct
date_month,
account_id,
account_name,
opp_id,
opp_name,
1 as has_churned_flag
from renewals_with_outcomes
where renewal_type = 'logo churn'
and to_date(date_month) <= date_trunc(month,to_date(current_date()))
and opp_id in (select distinct opp_id from renewal_opps)
order by date_month asc
),

/* opps that have churned but account is still active */
/* only maters for accounts ith multiple opps active at the same time */
mult_opp_accts_with_churn as (
select distinct
date_month,
account_id,
account_name,
opp_id,
opp_name,
prior_opp_id,
is_active_acct
from renewals_with_outcomes
where renewal_type = 'arr decrease'
and prior_opp_id IS NULL 
and is_active_acct = true
and to_date(date_month) <= date_trunc(month,to_date(current_date()))
and opp_id in (select distinct opp_id from renewal_opps)
order by date_month asc
),

/* Renewals that are past their renewal date */
outstanding_renewals as (
select distinct
account_id,
account_name,
opp_id,
opp_name,
1 as outstanding_renewal_flag
from renewal_opps
where to_date(end_dte) < to_date(current_date()) 
and opp_id not in (select distinct opp_id from churn)
and opp_id not in (select distinct prior_opp_id from renewals)
and opp_id not in (select distinct opp_id from mult_opp_accts_with_churn)
),

/* Upcoming renewals */
upcoming_renewals as (
select distinct
account_id,
account_name,
opp_id,
opp_name,
1 as upcoming_renewal_flag
from renewal_opps
where to_date(end_dte) >= to_date(current_date())
and opp_id not in (select distinct opp_id from churn)
and opp_id not in (select distinct prior_opp_id from renewals)
and opp_id not in (select distinct opp_id from mult_opp_accts_with_churn)
),

/* Combining all the above datasets */
fct_renewals_int as (
select distinct
ro.date_month as renewal_month,
ro.qtr_end_dte,
ro.account_id,
ro.account_name,
ro.opp_id as existing_opp_id,
ro.opp_name as existing_opp_name,
aoh.arr as potential_churn_amount,
ZEROIFNULL(r.renewal_month) as has_account_renewed_flag,
rwo.renewal_type,
CASE WHEN rwo.renewal_type = 'logo churn' then (-1*potential_churn_amount) else rwo.renewal_diff end as renewal_arr_change,
rwo.renewal_opp_id,
rwo.renewal_opp_name,
rwo.actual_renewal_amount,
ZEROIFNULL(rwo.renewal_with_arr_churn_flag) as renewal_with_arr_churn_flag,
ZEROIFNULL(rwo.renewal_with_arr_expansion_flag) as renewal_with_arr_expansion_flag, 
ZEROIFNULL(rwo.flat_renewal_flag) as flat_renewal_flag, 
ZEROIFNULL(rwo.logo_churn_flag) as logo_churn_flag,
ZEROIFNULL(rwo.renewal_flag) as renewal_flag,
ZEROIFNULL(c.has_churned_flag) as has_churned_flag,
ZEROIFNULL(o_r.outstanding_renewal_flag) as outstanding_renewal_flag,
ZEROIFNULL(ur.upcoming_renewal_flag) as upcoming_renewal_flag,
ro.end_dte as existing_opp_renewal_date
from renewal_opps as ro
left join renewals as r on (ro.opp_id = r.opp_id)
left join renewals_with_outcomes as rwo on (ro.opp_id = rwo.opp_id)
left join churn as c on (ro.opp_id = c.opp_id)
left join outstanding_renewals as o_r on (ro.opp_id = o_r.opp_id)
left join upcoming_renewals as ur on (ro.opp_id = ur.opp_id)
left join arr_opp_history as aoh on (ro.opp_id = aoh.opp_id)
order by ro.date_month asc
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
CASE WHEN opp_id = '006Dm000002e67VIAQ' then '0061R00000zAI8KQAW' /* Virginia DMV */ 
     WHEN opp_id = '0061R00001A6iWpQAJ' then '0061R000016kGCyQAM' /* WRK */
     else prior_opp_id end as prior_opp_id,
opp_close_dte
from "DEV"."SALES"."SALESFORCE_AGG_OPPORTUNITY"
where opp_stage_name not in ('Closed Won','Opp DQed','Closed Lost')
and opp_id not in ('006Dm0000046tabIAA') /* Removing Mutual of Omaha SaaS Migration Opp since another open opp is already open for the renewal */
and is_deleted = false
),

open_opps as (
select * 
from open_opps_int
where prior_opp_id in (select distinct existing_opp_id from fct_renewals_int where outstanding_renewal_flag = 1 or upcoming_renewal_flag = 1)  
),

health_scores as (
select distinct
id as opp_id,
health_score_c as health_score
from "FIVETRAN_DATABASE"."SALESFORCE"."OPPORTUNITY"
where is_deleted = 'FALSE'
and _fivetran_active = 'TRUE'
),

account_region_lu as (
select distinct
account_id,
sales_region
from {{ref('account_sales_region_lu')}} 
),

fct_renewals as (
select distinct
fri.renewal_month,
fri.qtr_end_dte,
fri.account_id,
fri.account_name,
fri.existing_opp_id,
fri.existing_opp_name,
CASE WHEN fri.existing_opp_id = '0061R000014wI4hQAE' then 20000 /* adjusting for peerstreet arr */
     WHEN fri.existing_opp_id = '0061R00000yFonNQAS' then 0 /* MetaSource */
     WHEN fri.existing_opp_id = '0061R00000zAI8KQAW' then 0 /* Virginina DMV */
     WHEN fri.existing_opp_id = '0061R00000uL8ylQAC' then 0 /* PMP */
     when fri.existing_opp_id = '0061R000019PUmDQAW' then 6600000 /* Dept of VA Combined opps */
     ELSE fri.potential_churn_amount end as potential_churn_amount,
fri.has_account_renewed_flag,
fri.renewal_type,
CASE WHEN fri.existing_opp_id = '0061R000014wI4hQAE' then 20000 else fri.renewal_arr_change end as renewal_arr_change,
fri.renewal_opp_id,
fri.renewal_opp_name,
fri.actual_renewal_amount,
fri.renewal_with_arr_churn_flag,
fri.renewal_with_arr_expansion_flag, 
fri.flat_renewal_flag, 
fri.logo_churn_flag,
fri.renewal_flag,
fri.has_churned_flag,
fri.outstanding_renewal_flag,
fri.upcoming_renewal_flag,
fri.existing_opp_renewal_date,
oo.opp_id as open_opp_id,
oo.opp_name as open_opp_name,
oo.opp_arr as open_opp_arr,
oo.opp_net_new_arr as open_opp_net_new_arr_raw,
oo.opp_close_dte as open_opp_close_dte,
oo.opp_stage_name as open_opp_stage_name,
oo.opp_commit_status as open_opp_commit_status,
CASE WHEN fri.existing_opp_id in ('0061R00000yFonNQAS','0061R00000uL8ylQAC') then 0 /* MetaSource and PMP */
     WHEN open_opp_id IS NULL AND (fri.outstanding_renewal_flag + fri.upcoming_renewal_flag) = 1 and fri.existing_opp_id not in ('0061R00000yFonNQAS','0061R00000uL8ylQAC') then round((-1)*potential_churn_amount) 
     WHEN open_opp_id IS NULL and (fri.outstanding_renewal_flag + fri.upcoming_renewal_flag) = 0 then NULL
     WHEN open_opp_id = '0061R00001A6iWpQAJ' then 0
     when open_opp_id = '0061R00001A5wigQAB' then 0
     when open_opp_id = '006Dm000002e67VIAQ' then 43225
     else round(((-1)*potential_churn_amount + open_opp_arr)) end as open_opp_net_new_arr,
CASE WHEN open_opp_net_new_arr > 0 then 'expansion'
     WHEN open_opp_net_new_arr < 0 and open_opp_id IS NULL then 'no open opp'
     WHEN open_opp_net_new_arr < 0 and open_opp_id IS NOT NULL then 'churn'
     WHEN open_opp_net_new_arr = 0 and (fri.outstanding_renewal_flag + fri.upcoming_renewal_flag) = 1 then 'flat'
     WHEN open_opp_net_new_arr IS NULL then NULL
     ELSE 'other' end as open_opp_projected_renewal_type,
hs.health_score,
CASE WHEN hs.health_score = 'Red' then 1 else 0 end as renewal_at_risk,
arl.sales_region
from fct_renewals_int as fri
left join open_opps as oo on (fri.existing_opp_id = oo.prior_opp_id)
left join health_scores as hs on (hs.opp_id = oo.opp_id)
left join account_region_lu as arl on (fri.account_id = arl.account_id)
where fri.existing_opp_id not in ('0061R00000yEQVgQAO', /* Removing GDVIT - VA because renewal bucketd with larger new opp */
                                  '0061R00001A6F76QAF' /* Removing WRK Upsell because renewal is asscoaited with Various Use Cases Opp */
                                  ) 
order by renewal_month asc
)

select * from fct_renewals
