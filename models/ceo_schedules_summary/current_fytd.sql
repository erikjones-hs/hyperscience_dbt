{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'SCHEDULES_SUMMARY'
)
}}

with actuals as (
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
where to_date(date_month) >= '2022-03-01'
order by date_month asc
),

last_fy_beg_arr as (
select distinct 
beginning_arr
from actuals
where fy_year = add_months(date_trunc(year,to_date(current_date())),-12)
and row_num_fy = 1
),

current_fy_beg_arr as (
select distinct 
beginning_arr
from actuals
where fy_year = date_trunc(year,to_date(current_date()))
and row_num_fy = 1
),

ytd_calc as (
select distinct
row_num_fy
from actuals
where fy_year = date_trunc(year,to_date(current_date()))
and date_month = add_months(date_trunc(month,to_date(current_date)),-1)
),

last_fy_totals as (
select
date_month,
(select * from last_fy_beg_arr) as beginning_arr,
sum(new_bookings) over (order by date_month asc rows between unbounded preceding and current row) as new_bookings,
sum(na_new_bookings) over (order by date_month asc rows between unbounded preceding and current row) as na_new_bookings,
sum(emea_new_bookings) over (order by date_month asc rows between unbounded preceding and current row) as emea_new_bookings,
sum(apac_new_bookings) over (order by date_month asc rows between unbounded preceding and current row) as apac_new_bookings,
sum(churn_arr) over (order by date_month asc rows between unbounded preceding and current row) as churn_arr,
sum(de_book_arr) over (order by date_month asc rows between unbounded preceding and current row) as de_book_arr,
sum(na_churn) over (order by date_month asc rows between unbounded preceding and current row) as na_churn_arr,
sum(emea_churn) over (order by date_month asc rows between unbounded preceding and current row) as emea_churn_arr,
sum(apac_churn) over (order by date_month asc rows between unbounded preceding and current row) as apac_churn_arr,
sum(net_new_arr) over (order by date_month asc rows between unbounded preceding and current row) as net_new_arr,
ending_arr
from actuals 
where fy_year = add_months(date_trunc(year,to_date(current_date())),-12)
and row_num_fy <= (select * from ytd_calc)
QUALIFY row_number() over (partition by fy_year order by date_month desc) = 1
),

current_fy_totals as (
select
date_month,
(select * from current_fy_beg_arr) as beginning_arr,
sum(new_bookings) over (order by date_month asc rows between unbounded preceding and current row) as new_bookings,
sum(na_new_bookings) over (order by date_month asc rows between unbounded preceding and current row) as na_new_bookings,
sum(emea_new_bookings) over (order by date_month asc rows between unbounded preceding and current row) as emea_new_bookings,
sum(apac_new_bookings) over (order by date_month asc rows between unbounded preceding and current row) as apac_new_bookings,
sum(churn_arr) over (order by date_month asc rows between unbounded preceding and current row) as churn_arr,
sum(de_book_arr) over (order by date_month asc rows between unbounded preceding and current row) as de_book_arr,
sum(na_churn) over (order by date_month asc rows between unbounded preceding and current row) as na_churn_arr,
sum(emea_churn) over (order by date_month asc rows between unbounded preceding and current row) as emea_churn_arr,
sum(apac_churn) over (order by date_month asc rows between unbounded preceding and current row) as apac_churn_arr,
sum(net_new_arr) over (order by date_month asc rows between unbounded preceding and current row) as net_new_arr,
ending_arr
from actuals 
where fy_year = date_trunc(year,to_date(current_date()))
and row_num_fy <= (select * from ytd_calc)
QUALIFY row_number() over (partition by fy_year order by date_month desc) = 1
),

combined as (
select * from last_fy_totals
UNION ALL 
select * from current_fy_totals
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
na_new_bookings,
na_new_bookings - lag(na_new_bookings,1,0) over(order by date_month asc) as na_new_bookings_yoy_change,
(na_new_bookings - lag(na_new_bookings,1,0) over(order by date_month asc)) / NULLIFZERO(lag(na_new_bookings,1,0) over(order by date_month asc)) as na_new_bookings_yoy_change_perc,
emea_new_bookings,
emea_new_bookings - lag(emea_new_bookings,1,0) over(order by date_month asc) as emea_new_bookings_yoy_change,
(emea_new_bookings - lag(emea_new_bookings,1,0) over(order by date_month asc)) / NULLIFZERO(lag(emea_new_bookings,1,0) over(order by date_month asc)) as emea_new_bookings_yoy_change_perc,
apac_new_bookings,
apac_new_bookings - lag(apac_new_bookings,1,0) over(order by date_month asc) as apac_new_bookings_yoy_change,
(apac_new_bookings - lag(apac_new_bookings,1,0) over(order by date_month asc)) / NULLIFZERO(lag(apac_new_bookings,1,0) over(order by date_month asc)) as apac_new_bookings_yoy_change_perc,
churn_arr,
abs(churn_arr) + lag(churn_arr,1,0) over(order by date_month asc) as churn_arr_yoy_change,
(abs(churn_arr) - abs(lag(churn_arr,1,0) over(order by date_month asc))) / NULLIFZERO(abs(lag(churn_arr,1,0) over(order by date_month asc))) as churn_arr_yoy_change_perc,                                          
na_churn_arr,
abs(na_churn_arr) + lag(na_churn_arr,1,0) over(order by date_month asc) as na_churn_arr_yoy_change,
(abs(na_churn_arr) - abs(lag(na_churn_arr,1,0) over(order by date_month asc))) / NULLIFZERO(abs(lag(na_churn_arr,1,0) over(order by date_month asc))) as na_churn_arr_yoy_change_perc,                                         
emea_churn_arr,
abs(emea_churn_arr) + lag(emea_churn_arr,1,0) over(order by date_month asc) as emea_churn_arr_yoy_change,
(abs(emea_churn_arr) - abs(lag(emea_churn_arr,1,0) over(order by date_month asc))) / NULLIFZERO(abs(lag(emea_churn_arr,1,0) over(order by date_month asc)))  as emea_churn_arr_yoy_change_perc,                                         
apac_churn_arr,
abs(apac_churn_arr) + lag(apac_churn_arr,1,0) over(order by date_month asc) as apac_churn_arr_yoy_change,
(abs(apac_churn_arr) - abs(lag(apac_churn_arr,1,0) over(order by date_month asc))) / NULLIFZERO(abs(lag(apac_churn_arr,1,0) over(order by date_month asc)))  as apac_churn_arr_yoy_change_perc,                                        
de_book_arr,
de_book_arr - lag(de_book_arr,1,0) over(order by date_month asc) as de_book_arr_yoy_change,
(de_book_arr - lag(de_book_arr,1,0) over(order by date_month asc)) / NULLIFZERO(lag(de_book_arr,1,0) over(order by date_month asc)) as de_book_arr_yoy_change_perc,                                         
net_new_arr,
net_new_arr - lag(net_new_arr,1,0) over(order by date_month asc) as net_new_arr_yoy_change,
(net_new_arr - lag(net_new_arr,1,0) over(order by date_month asc)) / NULLIFZERO(abs(lag(net_new_arr,1,0) over(order by date_month asc)))  as net_new_arr_yoy_change_perc,                                        
ending_arr,
ending_arr - lag(ending_arr,1,0) over(order by date_month asc) as ending_arr_yoy_change,
(ending_arr - lag(ending_arr,1,0) over(order by date_month asc)) / NULLIFZERO(lag(ending_arr,1,0) over(order by date_month asc)) as ending_arr_yoy_change_perc                                          
from combined
)

select * from yoy_diff where date_month = add_months(date_trunc(month,to_date(current_date)),-1)