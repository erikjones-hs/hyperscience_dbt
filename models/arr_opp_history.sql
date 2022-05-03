{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

/* Merging historical data set with incrementa closed won opps */
/* This is now the full closed won opportunity data set that will be transformed */ 
with raw_data as (
select * from {{ref('arr_opp_history_1')}}
UNION 
select * from {{ref('arr_opp_history_2')}}
UNION 
select * from {{ref('arr_opp_history_3')}} 
UNION 
select * from {{ref('arr_opp_history_4')}} 
UNION 
select * from {{ref('arr_opp_history_5')}}
UNION 
select * from {{ref('arr_opp_history_6')}}
UNION 
select * from {{ref('arr_opp_history_7')}}
UNION 
select * from {{ref('arr_opp_history_8')}}
UNION 
select * from {{ref('arr_opp_history_9')}}
UNION 
select * from {{ref('arr_opp_history_10')}} 
UNION 
select * from {{ref('arr_opp_history_11')}}
UNION 
select * from {{ref('arr_opp_history_12')}}  
UNION 
select * from {{ref('arr_opp_history_13')}}
UNION 
select * from {{ref('arr_opp_history_14')}}
UNION 
select * from {{ref('arr_opp_history_15')}}
UNION 
select * from {{ref('arr_opp_history_16')}}
UNION 
select * from {{ref('arr_opp_history_17')}}
UNION 
select * from {{ref('arr_opp_history_18')}}
UNION 
select * from {{ref('arr_opp_history_19')}}
order by account_id, start_dte_raw asc
)

select * from raw_data