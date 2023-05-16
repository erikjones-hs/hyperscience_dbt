{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'RENEWALS'
)
}}

/* Renewals with outcome dataset */
/* This only looks at renewals where a decision has been made (i.e. not including outstanding renewals or upcoming renewals) */
/* Merging in ARR from fct arr opp for both old and new opp to calculate if the renewal was decrease, flat or expansion */
with renewals_with_outcomes as (
select distinct
ro.date_month,
ro.account_id,
ro.account_name,
ro.opp_id,
ro.opp_name,
ro.end_dte,
fao1.opp_arr as potential_churn_amount,
ro.qtr_end_dte,
r.opp_id as renewal_opp_id,
r.opp_name as renewal_opp_name,
r.prior_opp_id,
fao2.opp_arr as actual_renewal_amount,
(round(actual_renewal_amount) - round(potential_churn_amount)) as renewal_diff,
faa.is_active_acct,
CASE WHEN renewal_diff = 0 then 'flat'
     WHEN renewal_diff < 0 then 'arr decrease'
     WHEN renewal_diff IS NULL and faa.is_active_acct = true then 'arr decrease'
     WHEN renewal_diff > 0 then 'arr increase'
     ELSE 'logo churn' end as renewal_type,
CASE WHEN renewal_type = 'arr decrease' then 1 else 0 end as renewal_with_arr_churn_flag,
CASE WHEN renewal_type = 'arr increase' then 1 else 0 end as renewal_with_arr_expansion_flag, 
CASE WHEN renewal_type = 'flat' then 1 else 0 end as flat_renewal_flag, 
CASE WHEN renewal_type = 'logo churn' then 1 else 0 end as logo_churn_flag,
CASE WHEN renewal_opp_id IS NOT NULL then 1 else 0 end as renewal_flag
from {{ref('opps_up_for_renewal')}} as ro
left join {{ref('renewed_opps')}} as r on (ro.opp_id = r.prior_opp_id)
left join {{ref('arr_opp_history')}} as fao1 on (ro.opp_id = fao1.opp_id)
left join {{ref('arr_opp_history')}} as fao2 on (r.opp_id = fao2.opp_id )
left join {{ref('fct_arr_account')}} as faa on (ro.account_id = faa.account_id and to_date(ro.date_month) = to_date(faa.date_month))
where date_trunc(month,to_date(ro.end_dte)) <= date_trunc(month,to_date(current_date()))
and ro.opp_id not in (select opp_id from {{ref('transformed_opps_for_open_negotiations')}})
order by ro.date_month asc
)

select * from renewals_with_outcomes