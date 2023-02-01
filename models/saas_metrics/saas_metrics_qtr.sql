{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'SAAS_METRICS'
)
}}

with budget as (
select * from {{ref('saas_metrics_qtr_budget')}}
),

forecast as (
select * from {{ref('saas_metrics_qtr_forecast')}} 
),

actuals as (
select * from {{ref('saas_metrics_qtr_actuals')}}      
),

saas_qtr_int as (
select distinct
b.qtr_end_dte,
b.metric,
b.budget,
f.forecast,
a.actuals
from budget as b
left join forecast as f on (f.qtr_end_dte = b.qtr_end_dte AND f.metric = b.metric)
left join actuals as a on (a.qtr_end_dte = b.qtr_end_dte AND a.metric = b.metric)
order by qtr_end_dte asc, b.metric 
),

saas_qtr as (
select distinct 
qtr_end_dte,
metric,
budget,
forecast,
actuals,
(actuals - budget) as budget_variance,
(actuals - forecast) as forecast_variance,
(budget_variance / nullifzero(budget)) as percent_budget_variance,
(forecast_variance / nullifzero(forecast)) as percent_forecast_variance
from saas_qtr_int
order by qtr_end_dte asc, metric
)

select * from saas_qtr
