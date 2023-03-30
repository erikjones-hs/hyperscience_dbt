{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'DATA_QC'
)
}} 

with prod_env as (
select distinct
domain,
CASE WHEN domain ilike '%prod.%' then 1 else 0 end as prod_env_flag,
split_part(domain,'.',0) as customer_int
from "HEAP_MAIN_PRODUCTION"."HEAP"."PAGEVIEWS"
where prod_env_flag = 1
),

saas_usage as (
select distinct
customer_int,
customer
from {{ref('saas_usage')}}
),

add_to_saas_usage as (
select distinct customer_int 
from prod_env
where customer_int not in (select customer_int from saas_usage)
)

select * from add_to_saas_usage