{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with acct_meta_data as (
select distinct
id as account_id,
name,
billing_country,
industry_cleaned_c as industry,
annual_revenue,
CASE WHEN name = 'PLUS Platform' then 'United States'
     WHEN name = 'Resound Corp' then 'Denmark'
     WHEN name = 'Featsystems' then 'India' 
     else billing_country end as billing_country_adjusted,
CASE WHEN billing_country_adjusted in ('France','Spain','Ireland','Switzerland','United Kingdom','Denmark','Germany') then 'EMEA'
     WHEN billing_country_adjusted in ('Australia','Korea','South Africa','India') then 'APAC'
     ELSE billing_country_adjusted end as region,
CASE WHEN annual_revenue > 0 and annual_revenue <100000000 then '1. <$100M'
     WHEN annual_revenue >= 100000000 and annual_revenue <500000000 then '2. $100M - $499M'
     WHEN annual_revenue >= 500000000 and annual_revenue <1000000000 then '3. $500M - $999.99M'
     WHEN annual_revenue >= 1000000000 and annual_revenue <5000000000 then '4. $1B - $4.99B'
     WHEN annual_revenue >= 5000000000 and annual_revenue <10000000000 then '5. $5B - 9.99B'
     WHEN annual_revenue >= 10000000000 then '6. $10B+'
     ELSE 'other' end as revenue_range
from "FIVETRAN_DATABASE"."SALESFORCE"."ACCOUNT"
)

select * from acct_meta_data

