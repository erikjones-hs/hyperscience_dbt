{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

/* Pulling in marketing influenced closed won data from marketing influenced closed won opps model */
with fct_marketing_influenced_int as (
select * from {{ref('marketing_influenced_closed_won_opps')}}
),

fy_dates as (
select distinct
dte,
month,
day_of_year,
day_of_qtr,
fy_quarter,
fy_year
from "DEV"."MARTS"."FY_CALENDAR"
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
opp_net_new_arr,
fy.dte,
fy.month,
fy.day_of_qtr,
fy.fy_quarter,
fy.fy_year
from fct_marketing_influenced_int
right join fy_dates as fy on (to_date(fct_marketing_influenced_int.closed_won_dte) = to_date(fy.dte))
where fy.dte >= '2018-01-01'
order by fy.dte asc
),

fy_agg as (
select 
fy_year,
to_timestamp(dte) as dte,
ZEROIFNULL(sum(opp_net_new_arr) over (partition by fy_year order by dte asc rows between unbounded preceding and current row)) as net_new_arr,
ZEROIFNULL(sum(opp_arr) over (partition by fy_year order by dte asc rows between unbounded preceding and current row)) as total_arr,
ZEROIFNULL(count(opp_id) over (partition by fy_year order by dte asc rows between unbounded preceding and current row)) as num_opps
from fct_marketing_influenced 
where to_date(dte) <= to_date(current_date())
order by fy_year asc, dte asc
)

select * from fy_agg