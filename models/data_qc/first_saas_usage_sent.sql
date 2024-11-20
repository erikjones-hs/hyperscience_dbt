{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'DATA_QC'
)
}}

with saas_usage as (
select distinct
split_part(report_id,'_',0) as customer_int,
min(to_timestamp(to_date(PERIOD_START))) as first_day_sent
from "RAW"."USAGE_REPORTING"."SAAS_PROD"
where (customer_int ilike '%-prod' or customer_int = 'momentum')
group by all 
order by 2 desc    
)

select * from saas_usage