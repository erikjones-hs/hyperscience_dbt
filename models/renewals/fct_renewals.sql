{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'RENEWALS'
)
}}

/* pulling in opportunities up for renewal */
with renewal_opps as (
select distinct
date_month,
account_id,
account_name,
opp_id,
opp_name,
CASE WHEN opp_id = '0061R000016kGCyQAM' then -90000
     WHEN opp_id = '0061R000014xeQwQAI' then -183115.38
     WHEN opp_id = '0061R00000r6r1iQAA' then -375000
     WHEN opp_id = '0061R00000zAI8KQAW' then -43225
     WHEN opp_id = '0061R000014wNrpQAE' then -35000.001
     else mrr_change end as mrr_change,
end_dte_raw as end_dte
from {{ref('fct_arr_opp_renewals')}}
where opp_category = 'churn'
order by end_dte asc
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
from {{ref('fct_arr_opp')}}
where customer_category = 'churn'
and to_date(date_month) <= date_trunc(month,to_date(current_date()))
and opp_id in (select distinct opp_id from renewal_opps)
order by date_month asc
),

/* opportunities that have renewed */
renewals as (
select *,
CASE WHEN opp_category = 'churn' and customer_category = 'active' then 1 else 0 end as renewal_month,
max(renewal_month) over (partition by account_id) as has_renewed,
sum(renewal_month) over (partition by account_id) as num_renewals,
CASE WHEN opp_id = '0061R000010tH9RQAU' then 0 else mrr_change_acct end as renewal_arr_change
from {{ref('fct_arr_opp')}} 
where renewal_month = 1
and to_date(date_month) <= dateadd(month,1,date_trunc(month,to_date(current_date())))
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
and opp_id not in (select distinct opp_id from renewals)
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
and opp_id not in (select distinct opp_id from renewals)
),

/* Combining all the above datasets */
fct_renewals as (
select distinct
ro.date_month as renewal_month,
ro.account_id,
ro.account_name,
ro.opp_id as existing_opp_id,
ro.opp_name as existing_opp_name,
ro.mrr_change as potential_churn_amount,
ZEROIFNULL(r.renewal_month) as has_renewed_flag,
r.renewal_arr_change as renewal_arr_change,
CASE WHEN renewal_arr_change > 0 then 'renewal_expansion'
     WHEN renewal_arr_change = 0 then 'renewal_flat'
     WHEN renewal_arr_change < 0 then 'renewal_arr_churn'
     ELSE 'other' end as renewal_type,
ZEROIFNULL(c.has_churned_flag) as has_churned_flag,
ZEROIFNULL(o_r.outstanding_renewal_flag) as outstanding_renewal_flag,
ZEROIFNULL(ur.upcoming_renewal_flag) as upcoming_renewal_flag,
ro.end_dte as existing_opp_renewal_date
from renewal_opps as ro
left join renewals as r on (ro.opp_id = r.opp_id)
left join churn as c on (ro.opp_id = c.opp_id)
left join outstanding_renewals as o_r on (ro.opp_id = o_r.opp_id)
left join upcoming_renewals as ur on (ro.opp_id = ur.opp_id)
order by ro.date_month asc
)

select * from fct_renewals
