{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'SCHEDULES_SUMMARY'
)
}}

with budget as (
select distinct
to_timestamp(to_date(date)) as dte,
recurring_arr_budget as beginning_budget,
new_arr_budget,
expansion_arr_budget,
(new_arr_budget + expansion_arr_budget) as new_bookings_budget,
churn_arr_budget,
(new_bookings_budget + churn_arr_budget) as net_new_arr_budget,
arr_budget as ending_arr_budget
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
where to_date(date) >= '2023-03-01' and to_date(date) < '2024-03-01'
order by dte asc
)

select * from budget

