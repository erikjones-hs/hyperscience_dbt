{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

/* Pulling in transformed SFDC data from arr_opp_history_transform model*/
with raw_data_transformed as (
select * from {{ref('arr_opp_history_transformed')}}
),

non_deleted_opps as (
select distinct 
bizible_2_opportunity_c
from "FIVETRAN_DATABASE"."SALESFORCE"."BIZIBLE_2_BIZIBLE_ATTRIBUTION_TOUCHPOINT_C"
where is_deleted = false
and bizible_2_opportunity_c is not null 
),

fct_marketing_influenced as (
select distinct
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
to_timestamp(end_dte) as end_dte,
to_timestamp(start_dte) as start_dte,  
to_timestamp(closed_won_dte) as closed_won_dte,
to_timestamp(start_dte_month) as start_dte_month,
to_timestamp(end_dte_month) as end_dte_month,
to_timestamp(closed_won_dte_month) as closed_won_dte_month,
opp_arr,
opp_net_new_arr
from raw_data_transformed
where opp_is_marketing_influenced_flag = 1
and opp_id in (select bizible_2_opportunity_c from non_deleted_opps)
order by account_id, start_dte asc
)

select * from fct_marketing_influenced