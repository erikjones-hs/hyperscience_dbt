{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}


with opp_partners as (
select distinct
opp_id,
opp_partner_account,
partner_account_name
from {{ ref('agg_opportunity_incremental') }}
where to_date(date_ran) = dateadd(day,-1,(to_date(current_date)))  
),

closed_won_opps as (
select distinct
date_month,
account_id,
account_name,
opp_id,
opp_name,
mrr as opp_arr,
mrr_acct as account_arr
from {{ ref('fct_arr_opp') }}
),

opps_partner as (
select distinct
cwo.date_month,
cwo.account_id,
cwo.account_name,
cwo.opp_id,
cwo.opp_name,
cwo.opp_arr,
cwo.account_arr,
op.opp_partner_account,
op.partner_account_name
from closed_won_opps as cwo 
left join opp_partners as op on (cwo.opp_id = op.opp_id)
order by cwo.account_id, cwo.date_month asc, cwo.opp_id 
)

select * from opps_partner