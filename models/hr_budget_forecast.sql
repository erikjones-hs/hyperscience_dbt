{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with headcount_budget as (
select distinct
to_timestamp(to_date(date)) as dte,
to_number(people_budget) as people,
to_number(operations_budget) as operations,
to_number(ml_budget) as ml,
to_number(product_budget) as product,
to_number(marketing_budget) as marketing,
to_number(cs_budget) as cs,
to_number(corp_dev_budget) as corp_dev,
to_number(engineering_budget) as engineering,
to_number(it_sec_budget) as it_sec,
to_number(finance_budget) as finance,
to_number(sales_budget) as sales_og,
to_number(legal_budget) as legal,
to_number((ml + product + engineering + it_sec)) as tech,
to_number((sales_og + corp_dev)) as sales
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
order by dte asc
),

headcount_forecast as (
select distinct
to_timestamp(to_date(date)) as dte,
to_number(product_forecast) as product,
to_number(finance_forecast) as finance,
to_number(ml_forecast) as ml,
to_number(cs_forecast) as cs,
to_number(legal_forecast) as legal,
to_number(people_forecast) as people,
to_number(marketing_forecast) as marketing,
to_number(operations_forecast) as operations,
to_number(sales_forecast) as sales_og,
to_number(corp_dev_forecast) as corp_dev,
to_number(engineering_forecast) as engineering,
to_number(it_sec_forecast) as it_sec,
to_number((ml + product + engineering + it_sec)) as tech,
to_number((sales_og + corp_dev)) as sales
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
order by dte asc
),

budget_unpivot_int as (
select *,
'budget' as category
from headcount_budget
unpivot(headcount for department in (product, finance, ml, cs, legal, people, marketing, operations, sales, engineering, it_sec, tech))
order by dte asc
),

budget_unpivot as (
select 
dte,
CASE WHEN department = 'ENGINEERING' then 'Engineering' 
     WHEN department = 'TECH' then 'Tech'
     WHEN department = 'IT_SEC' then 'IT Security'
     WHEN department = 'FINANCE' then 'Finance'
     WHEN department = 'ML' then 'ML'
     WHEN department = 'PRODUCT' then 'Product'
     WHEN department = 'CS' then 'Customer Success'
     WHEN department = 'PEOPLE' then 'People'
     WHEN department = 'MARKETING' then 'Marketing'
     WHEN department = 'OPERATIONS' then 'Operations'
     WHEN department = 'SALES' then 'Sales'
     WHEN department = 'LEGAL' then 'Legal'
     ELSE 'Other' end as department,
category,
headcount
from budget_unpivot_int
order by dte asc, department
),


forecast_unpivot_int as (
select *,
'forecast' as category
from headcount_forecast
unpivot(headcount for department in (product, finance, ml, cs, legal, people, marketing, operations, sales, engineering, it_sec, tech))
order by dte asc
),

forecast_unpivot as (
select 
dte,
CASE WHEN department = 'ENGINEERING' then 'Engineering' 
     WHEN department = 'TECH' then 'Tech'
     WHEN department = 'IT_SEC' then 'IT Security'
     WHEN department = 'FINANCE' then 'Finance'
     WHEN department = 'ML' then 'ML'
     WHEN department = 'PRODUCT' then 'Product'
     WHEN department = 'CS' then 'Customer Success'
     WHEN department = 'PEOPLE' then 'People'
     WHEN department = 'MARKETING' then 'Marketing'
     WHEN department = 'OPERATIONS' then 'Operations'
     WHEN department = 'SALES' then 'Sales'
     WHEN department = 'LEGAL' then 'Legal'
     ELSE 'Other' end as department,
category,
headcount
from forecast_unpivot_int
order by dte asc, department
),


budget_forecast as (
select * from budget_unpivot
UNION 
select * from forecast_unpivot
order by dte asc, department
),

fy_budget_forecast as (
select * 
from budget_forecast where to_date(dte) >= '2022-03-01'
order by dte asc, department, category
)

select * from fy_budget_forecast