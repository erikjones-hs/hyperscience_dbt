{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'CUSTOMER_USAGE'
)
}}

with opps as (
select distinct
opp_id,
opp_name,
account_name,
account_id
from {{ref('fct_arr_opp')}}
),

go_live_dates as (
select distinct
id,
actual_go_live_date_c as go_live_date
from "FIVETRAN_DATABASE"."SALESFORCE"."OPPORTUNITY"
where is_deleted = FALSE
),

acct_churn_dates as (
select distinct 
date_month as churn_month,
account_id,
account_name
from {{ref('fct_arr_account')}}
where customer_category = 'churn'
),

start_dates as (
select distinct
account_id,
account_name,
opp_id,
opp_name,
to_timestamp(start_dte) as start_date,
row_number() over (partition by account_id order by start_dte asc) as row_num
from {{ref('fct_arr_opp')}} 
qualify row_num = 1
),

opp_meta_data as (
select distinct
opp_id,
account_id,
account_name,
opp_close_dte,
opp_revenue_type,
opp_closed_won_dte
from "DEV"."SALES"."SALESFORCE_AGG_OPPORTUNITY"
),

fct_go_live_int as (
select distinct
o.account_id,
o.account_name,
o.opp_id,
o.opp_name,
gld.go_live_date,
acd.churn_month,
sd.start_date,
omd.opp_revenue_type,
omd.opp_close_dte,
omd.opp_closed_won_dte
from opps as o
left join go_live_dates as gld on (o.opp_id = gld.id)
left join acct_churn_dates as acd on (o.account_id = acd.account_id)
left join start_dates as sd on (o.account_id = sd.account_id)
left join opp_meta_data as omd on (o.opp_id = omd.opp_id)
where gld.go_live_date IS NOT NULL
order by account_name 
),

fct_go_live as (
select distinct
account_id,
account_name,
opp_id,
opp_name,
to_timestamp(go_live_date) as go_live_date,
churn_month,
start_date,
row_number() over (partition by account_id order by go_live_date asc) as row_num
from fct_go_live_int
qualify row_num = 1
),

dates as (
select distinct
dte
from "DEV"."MARTS"."FY_CALENDAR"
),

history_dates as (
select distinct
dates.dte,
fgl.account_id,
fgl.account_name,
fgl.opp_id,
fgl.opp_name,
fgl.go_live_date,
fgl.churn_month,
fgl.start_date
from fct_go_live as fgl
inner join dates on (dates.dte >= to_date(fgl.go_live_date) AND dates.dte <= to_date(fgl.churn_month))
order by account_id, dte asc
),

fct_go_live_history as (
select distinct
to_timestamp(dte) as dte,
account_id,
account_name,
opp_id,
opp_name,
go_live_date,
start_date,
churn_month,
CASE WHEN to_date(dte) < to_date(churn_month) and to_date(dte) >= go_live_date then 1 else 0 end as live_customer_fl,
CASE WHEN to_date(dte) = to_date(go_live_date) then 1 else 0 end as go_live_date_fl,
CASE WHEN datediff(day, to_date(start_date), to_date(go_live_date)) < 1 then 1 else datediff(day, to_date(start_date), to_date(go_live_date)) end as ttv_days,
(ttv_days / 31) as ttv_months
from history_dates 
order by account_id, dte asc
)

select * from fct_go_live_history