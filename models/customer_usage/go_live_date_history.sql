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
from {{ ref('fct_arr_opp') }}
),

go_live_dates as (
select distinct
id,
actual_go_live_date_c as go_live_date
from "FIVETRAN_DATABASE"."SALESFORCE"."OPPORTUNITY"
where is_deleted = FALSE
and _fivetran_active = 'TRUE' 
),

acct_churn_dates as (
select distinct 
date_month as churn_month,
account_id,
account_name
from {{ ref('fct_arr_account') }}
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
from {{ ref('fct_arr_opp') }} 
qualify row_num = 1
),

fct_go_live_int as (
select distinct
o.account_id,
o.account_name,
o.opp_id,
o.opp_name,
gld.go_live_date,
acd.churn_month,
sd.start_date
from opps as o
left join go_live_dates as gld on (o.opp_id = gld.id)
left join acct_churn_dates as acd on (o.account_id = acd.account_id)
left join start_dates as sd on (o.account_id = sd.account_id)
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

meta_data as (
select distinct
account_id,
deployment
from {{ ref('acct_meta_data') }}  
),

fct_go_live_history as (
select distinct
to_timestamp(hd.dte) as dte,
hd.account_id,
hd.account_name,
hd.opp_id,
hd.opp_name,
hd.go_live_date,
hd.start_date,
hd.churn_month,
CASE WHEN to_date(hd.dte) < to_date(churn_month) and to_date(hd.dte) >= go_live_date then 1 else 0 end as live_customer_fl,
CASE WHEN to_date(hd.dte) = to_date(hd.go_live_date) then 1 else 0 end as go_live_date_fl,
CASE WHEN datediff(day, to_date(hd.start_date), to_date(hd.go_live_date)) < 1 then 1 else datediff(day, to_date(hd.start_date), to_date(hd.go_live_date)) end as ttv_days,
(ttv_days / 31) as ttv_months,
md.deployment
from history_dates as hd 
left join meta_data as md on (md.account_id = hd.account_id)
order by account_id, dte asc
)

select * from fct_go_live_history