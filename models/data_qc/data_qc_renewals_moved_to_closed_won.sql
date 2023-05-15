{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'DATA_QC'
)
}}

with renewal_opps as (
select distinct
renewal_month,
account_id,
account_name,
existing_opp_id,
existing_opp_name,
potential_churn_amount,
has_churned_flag,
outstanding_renewal_flag,
upcoming_renewal_flag,
open_opp_id,
open_opp_name,
open_opp_close_dte
from {{ref('renewals_current_fy_opps')}}
where to_date(renewal_month) >= date_trunc(month,to_date(current_date()))
),

closed_won_opps as (
select * 
from {{ref('agg_opportunity_incremental')}}
where to_date(date_ran) = dateadd(day,-1,(to_date(current_date)))
and (opp_stage_name = 'Closed Won')  
),

renewals_moved_to_closed_won as (
select *
from renewal_opps
where open_opp_id in (select distinct opp_id from closed_won_opps)
order by renewal_month asc
)

select * from renewals_moved_to_closed_won 