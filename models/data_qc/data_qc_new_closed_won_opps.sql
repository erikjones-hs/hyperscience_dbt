{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'DATA_QC'
)
}}

with closed_won_n2 as (
select * 
from {{ref('agg_opportunity_incremental')}}
where to_date(date_ran) = dateadd(day,-2,(to_date(current_date))) 
and (opp_stage_name = 'Closed Won')  
),

closed_won_n1 as (
select * 
from {{ref('agg_opportunity_incremental')}}
where to_date(date_ran) = dateadd(day,-1,(to_date(current_date))) 
and (opp_stage_name = 'Closed Won')  
),

newly_closed_won as (
select * from closed_won_n1
where opp_id not in (select opp_id from closed_won_n2)
)

select * from newly_closed_won