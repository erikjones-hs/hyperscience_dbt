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

last_fy_totals as (
select
to_timestamp(to_date('2023-02-01')) as date_month,
(select * from last_fy_beg_arr) as beginning_arr,
sum(new_bookings) as last_fy_new_bookings,
sum(na_new_bookings) as last_fy_na_new_bookings,
sum(emea_new_bookings) as last_fy_emea_new_bookings,
sum(apac_new_bookings) as last_fy_apac_new_bookings,
sum(churn_arr) as last_fy_churn_arr,
sum(de_book_arr) as last_fy_de_book_arr,
sum(na_churn) as last_fy_na_churn_arr,
sum(emea_churn) as last_fy_emea_churn_arr,
sum(apac_churn) as last_fy_apac_churn_arr,
sum(net_new_arr) as last_fy_net_new_arr,
35783753 as ending_arr
from actuals 
where fy_year = '2022-01-01'
)

select * from last_fy_totals


