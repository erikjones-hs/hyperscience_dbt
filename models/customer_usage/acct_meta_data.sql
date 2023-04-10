{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'CUSTOMER_USAGE'
)
}}

/* Defining the currently active opp at the ACCOUNT level */
/* Currently Active Opp = Currently Active Opp with HIGHEST ARR */
/* Currently Actve Opp used to define contract length */
/* Taking ACCOUNT ARR as current month ARR */
with raw_data_int as (
select distinct 
fao.account_id,
fao.account_name,
fao.opp_id,
fao.opp_name,
fao.start_dte,
fao.end_dte,
CASE WHEN fao.end_dte >= to_date(current_date()) then 1 else 0 end as is_opp_active_fl,
faa.mrr_acct as arr,
row_number() over (partition by fao.account_id order by fao.start_dte desc, fao.mrr_acct desc) as row_num
from {{ref('fct_arr_opp')}} as fao
left join {{ref('fct_arr_account')}} as faa on (fao.account_id = faa.account_id AND date_trunc(month,current_date()) = to_date(faa.date_month))
where to_date(fao.date_month) <= date_trunc(month,to_date(current_date()))
qualify row_num = 1
),

raw_data as (
select distinct 
account_id,
account_name,
opp_id,
opp_name,
start_dte,
end_dte,
datediff(day,start_dte,end_dte) as contract_length_days,
datediff(month,start_dte,end_dte) as contract_length_months,
is_opp_active_fl, 
arr
from raw_data_int
order by opp_id, start_dte asc
),

/* Defining contracted pages as the pages contracted on the current active opp with MAX pages */
/* THIS ONLY MATTERS BECAUSE ALL OF THIS IS AT THE ACCOUNT LEVEL */
/* NEED SEPARATE PROCESS FOR ANYTHING AT THE OPP LEVEL */
contracted_pages as (
select distinct
opp_id,
opp_name,
account_id,
account_name,
start_dte,
end_dte,
contracted_pages as contract_pages_annual,
is_active,
row_number() over (partition by account_id order by start_dte desc, contracted_pages desc) as row_num
from {{ref('opp_contracted_pages_history')}}
qualify row_num = 1
order by account_id 
),

usage_meta_data as (
select distinct
account_id,
activated_customer_fl,
live_customer_fl
from "PROD"."CUSTOMER_USAGE"."USAGE_META_DATA"
where to_date(dte_month) = date_trunc(month,to_date(current_date()))
),

deployment_type as (
select distinct
id,
saa_s_or_on_prem_c as deployment
from "FIVETRAN_DATABASE"."SALESFORCE"."ACCOUNT"
),

fct_account_meta_data as (
select distinct
rd.account_id,
rd.account_name,
rd.opp_id,
rd.opp_name,
rd.start_dte,
rd.end_dte,
CASE WHEN rd.contract_length_months in (11,12,13) then 12
     WHEN rd.contract_length_months in (35,36,37) then 36 
     ELSE rd.contract_length_months end as contract_length_months,
rd.arr,
rd.is_opp_active_fl,
CASE WHEN rd.account_id = '0013600000QfzD9AAJ' then 999999999999 else cp.contract_pages_annual end as contract_pages_annual,
CASE WHEN umd.live_customer_fl IS NULL then 0 else umd.live_customer_fl end as live_customer_fl,
CASE WHEN umd.activated_customer_fl IS NULL then 0 else umd.activated_customer_fl end as activated_customer_fl ,
CASE WHEN dt.deployment IS NULL then 'On-Prem' else dt.deployment end as deployment
from raw_data as rd
left join contracted_pages as cp on (rd.opp_id = cp.opp_id)
left join usage_meta_data as umd on (rd.account_id = umd.account_id)
left join deployment_type as dt on (dt.id = rd.account_id)
order by rd.opp_name
)

select * from fct_account_meta_data