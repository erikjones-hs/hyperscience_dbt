{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'DATA_QC'
)
}}

with saas_usage as (
select 
split_part(report_id,'_',0) as customer,
max(to_date(period_start)) as most_recent_day_sent
from "RAW"."USAGE_REPORTING"."SAAS_PROD"
where (customer ilike '%-prod' or customer = 'momentum') 
group by customer
order by most_recent_day_sent desc
)

select * from saas_usage