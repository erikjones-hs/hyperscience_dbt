{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'CUSTOMER_USAGE'
)
}}

with raw_data_int as (
select distinct
fao.account_id,
fao.account_name,
fao.opp_id,
fao.opp_name,
CASE WHEN fao.opp_id = '0061R00000zD2sxQAC' then to_date('2022-03-15') else fao.start_dte end as start_dte,
fao.end_dte,
CASE WHEN fao.end_dte_month >= date_trunc(month,to_date(current_date())) then 1 else 0 end as is_opp_active_fl, 
faa.mrr_acct as arr
from {{ref('fct_arr_opp')}} as fao
left join {{ref('fct_arr_account')}} as faa on (fao.account_id = faa.account_id AND date_trunc(month,current_date()) = to_date(faa.date_month))
order by fao.opp_id, start_dte asc
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

contracted_pages as (
select distinct
opp_id,
opp_name,
sfdc_account_id,
sfdc_account_name,
contract_pages_annual
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."SFDC_CONTRACTED_PAGES_LOOKUP"
order by sfdc_account_id
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
cp.contract_pages_annual
from raw_data as rd
left join contracted_pages as cp on (rd.opp_id = cp.opp_id)
order by rd.opp_name
)

select * from fct_account_meta_data