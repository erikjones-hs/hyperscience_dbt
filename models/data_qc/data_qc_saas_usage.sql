{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'DATA_QC'
)
}} 

with raw_data_on_prem as (
select distinct
min((to_date(cd.date))) as dte,
cd.customer,
usl.sfdc_account_id,
usl.sfdc_account_name 
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."CUSTOMER_DATA" as cd 
left join "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."USAGE_SFDC_LOOKUP_ACCOUNT_LEVEL" as usl on (trim(cd.customer) = usl.customer_usage_data)  
where usl.customer_usage_data IS NULL
group by cd.customer, usl.sfdc_account_id, usl.sfdc_account_name
order by customer, dte desc
),

raw_data_on_prem_v1 as (
select distinct
min((to_date(cd.date))) as dte,
cd.customer,
usl.sfdc_account_id,
usl.sfdc_account_name 
from FIVETRAN_DATABASE.GOOGLE_SHEETS.CUSTOMER_DATA_V_1 as cd 
left join "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."USAGE_SFDC_LOOKUP_ACCOUNT_LEVEL" as usl on (trim(cd.customer) = usl.customer_usage_data)  
where usl.customer_usage_data IS NULL
group by cd.customer, usl.sfdc_account_id, usl.sfdc_account_name
order by customer, dte desc    
),

on_prem_prod as (
select distinct
min((to_date(cd.date))) as dte,
split_part(cd.customer,'-',0) as customer,
usl.sfdc_account_id,
usl.sfdc_account_name 
from RAW.USAGE_REPORTING.ON_PREM_PROD as cd
left join "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."USAGE_SFDC_LOOKUP_ACCOUNT_LEVEL" as usl on (trim(split_part(cd.customer,'-',0)) = usl.customer_usage_data)  
where usl.customer_usage_data IS NULL
and customer not in ('Hyper Labs Test-001Ou000007urbtIAA','Hyper Labs, Inc. (Internal Only)-0011R00002jTW74QAG','Hewlett Packard Enterprise (Partner)')
group by cd.customer, usl.sfdc_account_id, usl.sfdc_account_name
order by customer, dte desc    
),

combined as (
select * from raw_data_on_prem
UNION
select * from raw_data_on_prem_v1 
UNION 
select * from on_prem_prod
)

select * from combined