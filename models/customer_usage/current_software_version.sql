{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'CUSTOMER_USAGE'
)
}}

with raw_data as (
select distinct
to_date(url.date) as dte,
url.customer,
split_part(url.software_version,'@',0) as software_version,
usl.sfdc_account_id as account_id,
usl.sfdc_account_name as account_name 
from {{ref('usage_report_full')}} as url
left join "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."USAGE_SFDC_LOOKUP_ACCOUNT_LEVEL" as usl on (url.customer = usl.customer_usage_data) 
where software_version IS NOT NULL
and software_version not in ('Unknown')
order by customer, dte desc
),

meta_data as (
select distinct
account_id,
is_active_acct
from {{ref('fct_arr_account')}}
where to_date(date_month) = date_trunc(month,to_date(current_date()))
),

fct_software_version_int as (
select distinct
rd.dte,
rd.customer,
rd.software_version,
left(rd.software_version,4) as version,
rd.account_id,
rd.account_name,
CASE WHEN rd.account_name in ('Conduent','AIG (American International Group, Inc)','Department of Veterans Affairs','Pacific Life') then rd.customer 
     ELSE rd.account_name end as customer_name,
row_number() over (partition by customer_name order by rd.dte desc) as row_num,
md.is_active_acct
from raw_data as rd
left join meta_data as md on (rd.account_id = md.account_id)
order by customer, dte desc
),

fct_software_version as (
select distinct
dte,
customer,
software_version,
version,
account_id,
account_name,
customer_name,
row_num,
is_active_acct
from fct_software_version_int
where row_num = 1
and is_active_acct = TRUE
order by customer_name
)

select * from fct_software_version