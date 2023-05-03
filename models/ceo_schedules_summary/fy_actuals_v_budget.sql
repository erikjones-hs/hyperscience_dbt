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

actuals as (
select distinct
rs.date_month,
rs.beginning_arr,
rs.new_bookings,
rs.na_new_bookings,
rs.emea_new_bookings,
rs.apac_new_bookings,
rs.churn_arr,
rs.na_churn,
rs.emea_churn,
rs.apac_churn,
rs.de_book_arr,
rs.net_new_arr,
rs.ending_arr,
CASE WHEN month in (1,2) then dateadd('year',-1,date_trunc(year,fc.dte)) ELSE date_trunc(year,fc.dte) end as fy_year,
row_number() over (partition by fy_year order by date_month asc) as row_num_fy
from {{ref('revenue_schedule')}} as rs
left join "DEV"."MARTS"."FY_CALENDAR" as fc on (rs.date_month = to_date(fc.dte))
where to_date(date_month) >= '2023-03-01'
order by date_month asc
),

fy_beg_arr as (
select distinct 
beginning_arr
from actuals
where row_num_fy = 1
),

current_fy_budget_arr as (
select distinct 
beginning_budget
from budget
where row_num_fy = 1
),

ytd_calc as (
select distinct
row_num_fy
from actuals
where date_month = add_months(date_trunc(month,to_date(current_date)),-1)
),

fy_totals as (
select
date_month,
(select * from fy_beg_arr) as beginning_arr,
sum(new_bookings) over (order by date_month asc rows between unbounded preceding and current row) as new_bookings,
sum(churn_arr) over (order by date_month asc rows between unbounded preceding and current row) as churn_arr,
sum(net_new_arr) over (order by date_month asc rows between unbounded preceding and current row) as net_new_arr,
ending_arr
from actuals 
where row_num_fy <= (select * from ytd_calc)
QUALIFY row_number() over (order by date_month desc) = 1
),

current_fy_budget_int as (
select
date_month,
(select * from current_fy_budget_arr) as beginning_arr,
sum(new_bookings_budget) over (order by date_month asc rows between unbounded preceding and current row) as new_bookings,
sum(churn_arr_budget) over (order by date_month asc rows between unbounded preceding and current row) as churn_arr,
sum(net_new_arr_budget) over (order by date_month asc rows between unbounded preceding and current row) as net_new_arr,
ending_arr_budget as ending_arr
from budget 
where row_num_fy <= (select * from ytd_calc)
QUALIFY row_number() over (partition by fy_year order by date_month desc) = 1
),

current_fy_budget as (
select distinct
add_months(date_month,-12) as date_month,
beginning_arr,
new_bookings,
churn_arr,
net_new_arr,
ending_arr
from current_fy_budget_int
),

combined as (
select * from fy_totals
UNION ALL 
select * from current_fy_budget
order by date_month asc
),

yoy_diff as (
select distinct
date_month,
beginning_arr,
beginning_arr - lag(beginning_arr,1,0) over(order by date_month asc) as beginning_arr_yoy_change,
(beginning_arr - lag(beginning_arr,1,0) over(order by date_month asc)) / NULLIFZERO(lag(beginning_arr,1,0) over(order by date_month asc)) as beginning_arr_yoy_change_perc,
new_bookings,
new_bookings - lag(new_bookings,1,0) over(order by date_month asc) as new_bookings_yoy_change,
(new_bookings - lag(new_bookings,1,0) over(order by date_month asc)) / NULLIFZERO(lag(new_bookings,1,0) over(order by date_month asc)) as new_bookings_yoy_change_perc,
churn_arr,
abs(churn_arr) + lag(churn_arr,1,0) over(order by date_month asc) as churn_arr_yoy_change,
(abs(churn_arr) - abs(lag(churn_arr,1,0) over(order by date_month asc))) / NULLIFZERO(abs(lag(churn_arr,1,0) over(order by date_month asc))) as churn_arr_yoy_change_perc,                                                                                  
net_new_arr,
net_new_arr - lag(net_new_arr,1,0) over(order by date_month asc) as net_new_arr_yoy_change,
(net_new_arr - lag(net_new_arr,1,0) over(order by date_month asc)) / NULLIFZERO(abs(lag(net_new_arr,1,0) over(order by date_month asc)))  as net_new_arr_yoy_change_perc,                                         
ending_arr,
ending_arr - lag(ending_arr,1,0) over(order by date_month asc) as ending_arr_yoy_change,
(ending_arr - lag(ending_arr,1,0) over(order by date_month asc)) / NULLIFZERO(lag(ending_arr,1,0) over(order by date_month asc)) as ending_arr_yoy_change_perc                                          
from combined
)

select * from yoy_diff where date_month = add_months(date_trunc(month,to_date(current_date)),-1) 

/* 
yoy_diff as (
select distinct
date_month,
beginning_arr,
beginning_arr - lag(beginning_arr,1,0) over(order by date_month asc) as beginning_arr_yoy_change,
(beginning_arr - lag(beginning_arr,1,0) over(order by date_month asc)) / NULLIFZERO(lag(beginning_arr,1,0) over(order by date_month asc)) as beginning_arr_yoy_change_perc,
new_bookings,
new_bookings - lag(new_bookings,1,0) over(order by date_month asc) as new_bookings_yoy_change,
(new_bookings - lag(new_bookings,1,0) over(order by date_month asc)) / NULLIFZERO(lag(new_bookings,1,0) over(order by date_month asc)) as new_bookings_yoy_change_perc,
churn_arr,
churn_arr - lag(churn_arr,1,0) over(order by date_month asc) as churn_arr_yoy_change,
(churn_arr - lag(churn_arr,1,0) over(order by date_month asc)) / NULLIFZERO(lag(churn_arr,1,0) over(order by date_month asc)) as churn_arr_yoy_change_perc,                                                                                  
net_new_arr,
net_new_arr - lag(net_new_arr,1,0) over(order by date_month asc) as net_new_arr_yoy_change,
(net_new_arr - lag(net_new_arr,1,0) over(order by date_month asc)) / NULLIFZERO(lag(net_new_arr,1,0) over(order by date_month asc))  as net_new_arr_yoy_change_perc,                                        
ending_arr,
ending_arr - lag(ending_arr,1,0) over(order by date_month asc) as ending_arr_yoy_change,
(ending_arr - lag(ending_arr,1,0) over(order by date_month asc)) / NULLIFZERO(lag(ending_arr,1,0) over(order by date_month asc)) as ending_arr_yoy_change_perc                                          
from combined
)
*/