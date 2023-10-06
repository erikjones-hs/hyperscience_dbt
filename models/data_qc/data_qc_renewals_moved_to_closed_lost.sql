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
outstanding_renewal_flag,
upcoming_renewal_flag,
open_opp_id,
open_opp_name
from {{ref('fct_renewals')}}
),

closed_lost_opps as (
select distinct 
clw.opp_id,
sao.prior_opp_id
from {{ref('closed_lost_won')}} as clw
left join "DEV"."SALES"."SALESFORCE_AGG_OPPORTUNITY" as sao on (clw.opp_id = sao.opp_id)
where clw.new_value = 'Closed Lost'  
),

renewals_moved_to_closed_lost as (
select *
from renewal_opps
where existing_opp_id in (select distinct prior_opp_id from closed_won_opps)
order by renewal_month asc
)

select * from renewals_moved_to_closed_lost 