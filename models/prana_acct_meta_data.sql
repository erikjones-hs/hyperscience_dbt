{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'PRODUCT_ANALYTICS'
)
}} 

with opp_pages as (
select distinct
opp.id as opp_id,
opp.account_id as sfdc_account_id,
opp.forecasted_page_volume_c as contracted_pages,
opp.go_live_goal_date_c as go_live_date, 
opp.actual_go_live_date_c as actual_go_live_date
from "FIVETRAN_DATABASE"."SALESFORCE"."OPPORTUNITY" as opp
where opp.stage_name = 'Closed Won'
and opp.is_deleted = 'FALSE'
order by opp_id),

agg_opp_pages as (
select 
sfdc_account_id,
sum(contracted_pages) as tot_pages_contracted,
min(actual_go_live_date) as go_live_date
from opp_pages
group by sfdc_account_id
order by sfdc_account_id
),

acct as (
select 
id as sfdc_account_id,
name as sfdc_account_name,
annual_revenue,
number_of_employees,
industry_cleaned_c
from "FIVETRAN_DATABASE"."SALESFORCE"."ACCOUNT"
order by sfdc_account_id
),

fct_contract_pages as (
select distinct
aop.sfdc_account_id,
acct.sfdc_account_name,
aop.tot_pages_contracted,
aop.go_live_date
from agg_opp_pages as aop
left join acct on (aop.sfdc_account_id = acct.sfdc_account_id)
order by sfdc_account_name
),

acct_current_arr as (
select distinct
account_id,
mrr_acct as arr
from {{ ref('fct_arr_account') }}
where to_date(date_month) = date_trunc(month,to_date(current_date()))
),

acct_meta_data as (
select distinct
account_id,
first_active_month as start_dte,
last_active_month as end_dte,
datediff(months,start_dte,to_date(current_date)) as months_since_start,
datediff(months,to_date(current_date),end_dte) as num_months_to_renewal
from from {{ ref('fct_arr_account') }} 
where start_dte IS NOT NULL
order by account_id
),

usage_lookup as (
select distinct
sfdc_account_id,
customer_name,
usage_prod_name
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."SAAS_USAGE_SFDC_LOOKUP"
),

fct_acct as (
select distinct
a.sfdc_account_id as account_id,
a.sfdc_account_name as account_name,
a.annual_revenue,
a.number_of_employees,
a.industry_cleaned_c,
amd.start_dte as contract_start_dte,
amd.end_dte as contract_end_dte,
amd.months_since_start,
amd.num_months_to_renewal,
aca.arr,
fcp.tot_pages_contracted,
fcp.go_live_date,
ul.usage_prod_name,
ul.customer_name as customer
from acct as a
left join acct_meta_data as amd on (a.sfdc_account_id = amd.account_id)
left join acct_current_arr as aca on (a.sfdc_account_id = aca.account_id)
left join fct_contract_pages as fcp on (a.sfdc_account_id = fcp.sfdc_account_id)
left join usage_lookup as ul on (a.sfdc_account_id = ul.sfdc_account_id)
where ul.usage_prod_name IS NOT NULL
order by a.sfdc_account_id asc
)

select * from fct_acct