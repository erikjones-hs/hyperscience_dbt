{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'CUSTOMER_USAGE'
)
}}

with raw_data as (
select distinct
account_id,
sfdc_account_name,
min(dte_month) as start_dte_pages_int,
max(end_dte) as end_dte_int,
date_trunc('month',start_dte_pages_int) as start_dte,
date_trunc('month',end_dte_int) as end_dte,
1 as total_pages_placeholder
from {{ref('fct_usage')}}
group by account_id, sfdc_account_name
),

months as (
select distinct
date_trunc('month',dte) as date_month
from "DEV"."MARTS"."FY_CALENDAR"
),

live_customer_months as (
select distinct
months.date_month,
rd.account_id,
rd.start_dte,
rd.end_dte,
rd.total_pages_placeholder
from raw_data as rd
inner join months on (months.date_month >= to_date(rd.start_dte) AND months.date_month < to_date(rd.end_dte))
order by account_id, date_month asc
),

joined_customer_months as (
select 
lcm.date_month,
lcm.account_id,
lcm.start_dte,
lcm.end_dte,
lcm.total_pages_placeholder,
coalesce(raw_data.total_pages_placeholder,0) as tot_pages_placeholder
from live_customer_months as lcm
left join raw_data on (lcm.account_id = raw_data.account_id AND lcm.date_month >= raw_data.start_dte AND lcm.date_month < raw_data.end_dte)
),

final_cust as (
select 
date_month,
account_id,
start_dte,
end_dte,
total_pages_placeholder,
tot_pages_placeholder,
to_date(end_dte) > to_date(date_month) as is_active,
min(case when is_active then date_month end) over (partition by account_id) as first_active_month,
max(case when is_active then date_month end) over (partition by account_id) as last_active_month,
first_active_month = date_month as is_first_month,
last_active_month = date_month as is_last_month
from joined_customer_months
),

churn_cust as (
select 
dateadd(month,1,date_month)::date as date_month,
account_id,
start_dte,
end_dte,
0::float as total_pages_placeholder,
0::float as tot_pages_placeholder,
false as is_active,
first_active_month,
last_active_month,
false as is_first_month,
false as is_last_month
from final_cust
where is_last_month
),

unioned_cust as (
select * from final_cust 
UNION ALL 
select * from churn_cust
),

acct_meta_data as (
select distinct
account_id,
sfdc_account_name
from raw_data
),

fct_cust as (
select 
to_timestamp(uc.date_month) as date_month,
uc.account_id,
amd.sfdc_account_name,
to_timestamp(uc.start_dte) as start_dte,
to_timestamp(uc.end_dte) as end_dte,
uc.total_pages_placeholder,
uc.tot_pages_placeholder,
CASE WHEN uc.is_active = true then 1 else 0 end as is_live_customer,
uc.is_active,
to_timestamp(uc.first_active_month) as first_active_month,
to_timestamp(uc.last_active_month) as last_active_month,
uc.is_first_month,
uc.is_last_month
from unioned_cust as uc
left join acct_meta_data as amd on (uc.account_id = amd.account_id)
order by account_id, date_month asc
)

select * from fct_cust