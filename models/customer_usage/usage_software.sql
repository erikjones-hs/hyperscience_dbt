{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'CUSTOMER_USAGE'
)
}}

with raw_data as (
select * from {{ref('usage_report_full')}}
),

/* Calculating Monthly Pages for Non-Blended Contracts */
non_blend_monthly_pages as (
select distinct
date_trunc('month',date) as dte_month,
customer,
sum(number_of_pages_created) as total_pages_created
from raw_data
where customer not in ('AIG','AIG - Internal Audit','Guardian','Voya')
group by dte_month, customer
order by customer, dte_month asc
),

/* Need to correct for Blended Pages Contracts */
blended_raw_data as (
select distinct
customer,
date_trunc(month,to_date(date)) as dte_month,
NUMBER_OF_PAGES_WITH_FIELDS_ON_THEM_COMPLETED as num_pages_with_fields_completed,
ZEROIFNULL(NUMBER_OF_PAGES_MATCHED_TO_FLEX_LAYOUTS_CREATED) as num_matched_semi_structured_pages,
ZEROIFNULL(NUMBER_OF_PAGES_MATCHED_TO_FORM_LAYOUTS_CREATED) as num_matched_structured_pages,
NUMBER_OF_PAGES_CREATED as num_pages_created
from raw_data
where customer in ('AIG','AIG - Internal Audit','Guardian','Voya')
order by customer, dte_month asc
),

/* Blended Pages Contract Calculations */
blended_pages as (
select distinct
customer,
to_timestamp(dte_month) as dte_month,
CASE WHEN customer in ('AIG','AIG - Internal Audit') then ((num_pages_with_fields_completed) + (num_matched_semi_structured_pages/3) + ((num_pages_created - num_pages_with_fields_completed)/4)) 
     WHEN customer = 'Guardian' then ((num_pages_with_fields_completed) + ((num_pages_created - num_pages_with_fields_completed)/16))
     WHEN customer = 'Voya' then (num_matched_structured_pages + num_matched_semi_structured_pages)
     ELSE NULL end as total_pages_created
from blended_raw_data
order by customer, dte_month asc
),

/* Aggregating Blended Pages to Monthly Grain */
blended_monthly_pages as (
select distinct
dte_month,
customer,
sum(total_pages_created) as total_pages_created
from blended_pages
group by dte_month, customer
order by customer, dte_month asc
),

/* Combining On-Prem and SaaS Usage Data */
usage_combined as (
select * from non_blend_monthly_pages
UNION
select * from blended_monthly_pages
order by customer, dte_month asc
),

/* Brinign in the Lookup Table between Customer Usage Data and SFDC */
usage_sfdc_lookup as (
select distinct
customer_usage_data,
sfdc_account_id,
sfdc_account_name 
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."USAGE_SFDC_LOOKUP_ACCOUNT_LEVEL"
),

fct_usage_int as (
select distinct
uc.customer,
uc.dte_month,
uc.total_pages_created,
usl.customer_usage_data,
usl.sfdc_account_id,
usl.sfdc_account_name,
CASE WHEN usl.sfdc_account_name in ('Conduent','AIG (American International Group, Inc)','Department of Veterans Affairs','Pacific Life') then usl.customer_usage_data 
     ELSE usl.sfdc_account_name end as customer_name
from usage_combined as uc
left join usage_sfdc_lookup as usl on (uc.customer = usl.customer_usage_data)
order by customer, dte_month asc
),

current_software_version as (
select distinct 
customer,
account_id,
account_name,
customer_name,
version
from {{ref('current_software_version')}}
),

fct_usage_software_version as (
select distinct
fui.dte_month,
fui.customer_usage_data,
fui.sfdc_account_id,
fui.sfdc_account_name,
fui.customer_name,
fui.total_pages_created,
csv.version
from fct_usage_int as fui 
left join current_software_version as csv on (fui.customer_name = csv.customer_name)
order by fui.customer_name, fui.dte_month asc
)

select * from fct_usage_software_version


