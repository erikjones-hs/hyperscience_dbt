{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'CUSTOMER_USAGE'
)
}}

/* Pulling in On-Prem Montly Pages */
with raw_data_on_prem as (
select distinct
customer,
dte_month,
total_pages_created
from "DEV"."CX"."CUSTOMER_DATA_FCT_MONTHLY_PAGES"
order by customer, dte_month asc
),

/* Pulling in Raw SaaS Customer Usage Data */
saas_usage_raw as (
select distinct
customer,
period_start,
number_of_pages_created
from "DEV"."ERIKJONES"."SAAS_USAGE"
order by customer, period_start asc
),

/* Aggregating SaaS Usage Data to Monthly */
saas_usage_monthly as (
select distinct
customer,
date_trunc(month,to_date(period_start)) as dte_month,
sum(number_of_pages_created) as total_pages_created
from saas_usage_raw
group by customer, dte_month
order by customer, total_pages_created
),

/* Combining On-Prem and SaaS Usage Data */
usage_combined as (
select * from raw_data_on_prem
UNION
select * from saas_usage_monthly
order by customer, dte_month asc
),

usage_sfdc_lookup as (
select distinct
customer_usage_data,
opp_id,
opp_name,
sfdc_account_id,
sfdc_account_name 
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."USAGE_SFDC_LOOKUP"
),

fct_usage_int as (
select distinct
uc.customer,
uc.dte_month,
uc.total_pages_created,
usl.customer_usage_data,
usl.opp_id,
usl.opp_name,
usl.sfdc_account_id,
usl.sfdc_account_name
from usage_combined as uc
left join usage_sfdc_lookup as usl on (uc.customer = usl.customer_usage_data)
order by customer, dte_month asc
),

fct_usage_int2 as (
select distinct
dte_month,
sfdc_account_name, 
opp_id,
opp_name,
sum(total_pages_created) as total_pages
from fct_usage_int
where opp_id IS NOT NULL
group by dte_month, sfdc_account_name, opp_id, opp_name
order by sfdc_account_name, dte_month asc
),

meta_data as (
select distinct 
account_id,
account_name,
opp_id,
opp_name,
start_dte,
end_dte,
contract_length_months,
arr,
is_opp_active_fl,
contract_pages_annual
from {{ref('acct_meta_data')}}
),

fct_usage as (
select distinct
fui2.dte_month,
fui2.opp_id,
fui2.opp_name,
md.account_id,
fui2.sfdc_account_name, 
fui2.total_pages,
md.start_dte,
md.end_dte,
md.contract_length_months,
md.arr,
md.is_opp_active_fl,
md.contract_pages_annual
from fct_usage_int2 as fui2
left join meta_data as md on (fui2.opp_id = md.opp_id)
order by fui2.sfdc_account_name, fui2.opp_id, fui2.dte_month asc 
)

select * from fct_usage