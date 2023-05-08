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
round(opp_arr) as arr
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

/* Renewals that are past their renewal date */
outstanding_renewals as (
select distinct
account_id,
account_name,
opp_id,
opp_name,
1 as outstanding_renewal_flag
from renewal_opps
where to_date(end_dte) < date_trunc(month,to_date(current_date())) 
and opp_id not in (select distinct opp_id from churn)
and opp_id not in (select distinct prior_opp_id from renewals)
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
where to_date(end_dte) >= date_trunc(month,to_date(current_date()))
and opp_id not in (select distinct opp_id from churn)
and opp_id not in (select distinct prior_opp_id from renewals)
),

/* Combining all the above datasets */
fct_renewals as (
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
)

select * from fct_renewals
