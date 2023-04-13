{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'DATA_QC'
)
}} 

with raw_data_saas as (
select distinct
min((to_date(cd.period_start))) as dte,
cd.customer,
usl.sfdc_account_id,
usl.sfdc_account_name 
from {{ref('saas_usage')}} as cd 
left join "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."USAGE_SFDC_LOOKUP_ACCOUNT_LEVEL" as usl on (cd.customer = usl.customer_usage_data)  
where usl.customer_usage_data IS NULL
group by cd.customer, usl.sfdc_account_id, usl.sfdc_account_name
order by customer, dte desc
)

select * from raw_data_saas