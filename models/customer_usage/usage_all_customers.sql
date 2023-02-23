{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'CUSTOMER_USAGE'
)
}} 

/* Combining On-Prem and SaaS Usage Data */
with usage_combined as (
select distinct
dte_month,
account_id as customer,
sfdc_account_name,
total_pages as total_pages_created,
is_opp_active_fl
from {{ref('fct_usage')}}
where is_opp_active_fl = 1
order by customer, dte_month asc
),

/* Calculating Average, STD. Deviation for SPC and 1st and Last Month Data Sent for Meta Data */
usage_spc_int as (
select distinct 
customer,
dte_month,
total_pages_created,
sfdc_account_name,
avg(total_pages_created) over (partition by customer) as mean_pages_processed,
stddev_samp(total_pages_created) over (partition by customer) as std_dev_pages_processed,
max(to_date(dte_month)) over (partition by customer) as latest_date_received,
min(to_date(dte_month)) over (partition by customer) as first_date_received
from usage_combined
order by customer, dte_month asc
),

/* Calculations for Flagging if a customer is up, down or in -between */
/* All the lag variables are used to look at consecutive points. For example, 3 consecutive points above the mean, 2 consecutive above UCL, etc. */ 
usage_spc as (
select distinct
customer, 
dte_month,
total_pages_created,
sfdc_account_name,
mean_pages_processed,
std_dev_pages_processed,
latest_date_received,
first_date_received,
(mean_pages_processed + std_dev_pages_processed) as ucl,
lag(ucl,1) over (partition by customer order by dte_month asc) as prev_ucl,
(mean_pages_processed - std_dev_pages_processed) as lcl,
(total_pages_created - mean_pages_processed) as diff_from_mean,
CASE WHEN diff_from_mean > 0 then 1 
     WHEN diff_from_mean = 0 then 0
     WHEN diff_from_mean < 0 then -1 end as diff_from_mean_direction, /* Is current month pages processed above, below or at the mean */
lag(diff_from_mean_direction,1) over (partition by customer order by dte_month asc) as prev_diff_from_mean_direction, /* Looking 1 month ago at difference from mean */
lag(diff_from_mean_direction,2) over (partition by customer order by dte_month asc) as prev2_diff_from_mean_direction, /* 2 months ago for difference from the mean */
lag(total_pages_created,1) over (partition by customer order by dte_month asc) as prev_pages_created,
lag(total_pages_created,2) over (partition by customer order by dte_month asc) as prev2_pages_created,
lag(total_pages_created,3) over (partition by customer order by dte_month asc) as prev3_pages_created,  
CASE WHEN (diff_from_mean_direction = prev_diff_from_mean_direction OR prev_diff_from_mean_direction IS NULL) then 0 else 1 end as cross_over_point_fl,
CASE WHEN total_pages_created - prev_pages_created > 0 then 1 
     WHEN total_pages_created - prev_pages_created = 0 then 0
     WHEN total_pages_created - prev_pages_created < 0 then -1 end as pages_minus_prev_pages,
CASE WHEN prev_pages_created - prev2_pages_created > 0 then 1 
     WHEN prev_pages_created - prev2_pages_created = 0 then 0
     WHEN prev_pages_created - prev2_pages_created < 0 then -1 end as prev_pages_minus_prev2_pages,
CASE WHEN prev2_pages_created - prev3_pages_created > 0 then 1 
     WHEN prev2_pages_created - prev3_pages_created = 0 then 0
     WHEN prev2_pages_created - prev3_pages_created < 0 then -1 end as prev2_pages_minus_prev3_pages 
from usage_spc_int
order by customer, dte_month asc
),

/* Adding in flags for different scenarios */
/* Rules: 2 consecutive points above UCl = Up, 1 point below UCL = Down */
/* 3 consecutive points on same side of the average line = long-term-trend up / down */
/* 3 Consecutive points in the sme direction = short-term trend = up / down */
/* Customer Direction = Single value for knowing usage "health" */
fct_spc_int as (
select distinct 
customer, 
dte_month,
total_pages_created,
sfdc_account_name,
prev_pages_created,
prev2_pages_created,
prev3_pages_created,
mean_pages_processed,
std_dev_pages_processed,
latest_date_received,
first_date_received,
ucl,
prev_ucl,
lcl,
diff_from_mean,
diff_from_mean_direction,
prev_diff_from_mean_direction,
prev2_diff_from_mean_direction,
(prev2_diff_from_mean_direction + prev_diff_from_mean_direction + diff_from_mean_direction) as direction_sum,
cross_over_point_fl,
(pages_minus_prev_pages + prev_pages_minus_prev2_pages + prev2_pages_minus_prev3_pages) as mom_direction_sum,
sum(cross_over_point_fl) over (partition by customer order by dte_month asc rows between unbounded preceding and current row) as num_cross_over_points,
CASE WHEN num_cross_over_points >= 1 and direction_sum >= 3 then 1 else 0 end as long_term_trend_up_fl,
CASE WHEN long_term_trend_up_fl >= 1 AND mom_direction_sum >= 3 then 1 else 0 end as short_term_trend_up_fl,
CASE WHEN num_cross_over_points >= 1 and direction_sum <= -3 then 1 else 0 end as long_term_trend_down_fl,
CASE WHEN long_term_trend_down_fl >= 1 AND mom_direction_sum <= -3 then 1 else 0 end as short_term_trend_down_fl,
CASE WHEN total_pages_created < lcl AND prev_pages_created IS NOT NULL then 1 else 0 end as down_fl,
CASE WHEN total_pages_created > ucl AND prev_pages_created > prev_ucl AND prev_pages_created IS NOT NULL then 1 else 0 end as up_fl,
(up_fl + long_term_trend_up_fl + short_term_trend_up_fl - down_fl - long_term_trend_down_fl - short_term_trend_down_fl) as customer_direction
from usage_spc 
order by customer, dte_month asc
),

fct_spc as (
select distinct
customer, 
dte_month,
total_pages_created,
sfdc_account_name,
prev_pages_created,
prev2_pages_created,
prev3_pages_created,
mom_direction_sum,
mean_pages_processed,
std_dev_pages_processed,
latest_date_received,
first_date_received,
ucl,
prev_ucl,
lcl,
diff_from_mean,
diff_from_mean_direction,
prev_diff_from_mean_direction,
prev2_diff_from_mean_direction,
direction_sum,
cross_over_point_fl,
num_cross_over_points,
long_term_trend_up_fl,
short_term_trend_up_fl,
long_term_trend_down_fl,
short_term_trend_down_fl,
down_fl,
up_fl,
customer_direction
from fct_spc_int
order by customer, dte_month asc
)

select * from fct_spc