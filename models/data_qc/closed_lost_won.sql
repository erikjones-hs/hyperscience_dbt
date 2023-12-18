{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'DATA_QC'
)
}}

with change_history as (
select distinct
to_timestamp(created_date) as updated_dte,
opportunity_id as opp_id,
field,
old_value,
new_value
from "FIVETRAN_DATABASE"."SALESFORCE"."OPPORTUNITY_FIELD_HISTORY"
where field in ('StageName')
and new_value in ('Closed Lost','Closed Won')
and to_date(created_date) >= dateadd(day,-30,(to_date(current_date))) 
)

select * from change_history