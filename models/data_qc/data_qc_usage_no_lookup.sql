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

combined as (
select * from raw_data_on_prem
UNION
select * from raw_data_on_prem_v1    
)

select * from combined 