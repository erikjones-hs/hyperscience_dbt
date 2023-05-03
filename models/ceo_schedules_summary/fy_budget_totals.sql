{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'SCHEDULES_SUMMARY'
)
}}

with budget as (
select 
fb.dte as date_month,
fb.beginning_budget,
fb.new_arr_budget,
fb.expansion_arr_budget,
fb.new_bookings_budget,
fb.churn_arr_budget,
fb.net_new_arr_budget,
fb.ending_arr_budget,
CASE WHEN month in (1,2) then dateadd('year',-1,date_trunc(year,fc.dte)) ELSE date_trunc(year,fc.dte) end as fy_year,
row_number() over (partition by fy_year order by fb.dte asc) as row_num_fy
from {{ref('fy_budget')}} as fb
left join "DEV"."MARTS"."FY_CALENDAR" as fc on (to_date(fb.dte) = to_date(fc.dte)) 
order by date_month asc
),

current_fy_beg_arr_budget as (
select distinct 
beginning_budget
from budget
where row_num_fy = 1
),

current_fy_ending_arr_budget as (
select distinct 
ending_arr_budget
from budget
where row_num_fy = 12
),

current_fy_budget as (
select
to_timestamp(fy_year) as fy_year,
(select * from current_fy_beg_arr_budget) as beginning_arr,
sum(new_bookings_budget) as new_bookings,
sum(churn_arr_budget) as churn_arr,
sum(net_new_arr_budget) as net_new_arr,
(select * from current_fy_ending_arr_budget) as ending_arr
from budget 
group by fy_year
)

select * from current_fy_budget