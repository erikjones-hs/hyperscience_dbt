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
CASE WHEN b.metric = 'ARR' then 1
     WHEN b.metric = 'REVENUE' then 2
     WHEN b.metric = 'NET_DOLLAR_RETENTION' then 3
     WHEN b.metric = 'GROSS_DOLLAR_RETENTION' then 4
     WHEN b.metric = 'NET_LOGO_RETENTION' then 5
     WHEN b.metric = 'GROSS_MARGIN' then 6
     WHEN b.metric = 'GROSS_MARGIN_PERCENT' then 7
     WHEN b.metric = 'CAC_PAYBACK_PERIOD' then 8
     when b.metric = 'CASH_CONVERSION_SCORE' then 9
     when b.metric = 'RULE_OF_40' then 10
     when b.metric = 'MAGIC_NUMBER' then 11
     when b.metric = 'LTV_TO_CAC' then 12
     when b.metric = 'FCF_MARGIN' then 13
     when b.metric = 'NET_NEW_ARR' then 14
     when b.metric = 'BURN_MULTIPLE' then 15
     when b.metric = 'AWS_EXPENSE' then 16
     when b.metric = 'SOFTWARE_PER_FTE' then 17
     when b.metric = 'REAL_ESTATE_EXPENSE' then 18
     when b.metric = 'R_AND_D_EXPENSE' then 19
     when b.metric = 'S_AND_M_EXPENSE' then 20
     when b.metric = 'G_AND_A_EXPENSE' then 21
     when b.metric = 'TOTAL_OPEX' then 22
     when b.metric = 'GAAP_REV_PER_FTE' then 23
     when b.metric = 'COMP_AND_BENEFITS_PER_REV' then 24
     when b.metric = 'ENG_FTE' then 25
     when b.metric = 'SALES_FTE' then 26
     when b.metric = 'CX_FTE' then 27
     when b.metric = 'MARKETING_FTE' then 28
     when b.metric = 'PRODUCT_FTE' then 29
     when b.metric = 'FINANCE_FTE' then 30
     when b.metric = 'PEOPLE_FTE' then 31
     when b.metric = 'LEGAL_FTE' then 32
     else NULL end as metric_order_by_column, 
CASE WHEN b.metric = 'ARR' then 'ARR'
     WHEN b.metric = 'REVENUE' then 'Revenue'
     WHEN b.metric = 'NET_DOLLAR_RETENTION' then 'Net Dollar Retention'
     WHEN b.metric = 'GROSS_DOLLAR_RETENTION' then 'Gross Dollar Retention'
     WHEN b.metric = 'NET_LOGO_RETENTION' then 'Net Logo Retention'
     WHEN b.metric = 'GROSS_MARGIN' then 'Gross Margin'
     WHEN b.metric = 'GROSS_MARGIN_PERCENT' then 'Gross Margin %'
     WHEN b.metric = 'CAC_PAYBACK_PERIOD' then 'CAC Payback Period (BVP Calc.)'
     when b.metric = 'CASH_CONVERSION_SCORE' then 'Cash Conversion Score'
     when b.metric = 'RULE_OF_40' then 'Rule of 40'
     when b.metric = 'MAGIC_NUMBER' then 'Magic Number'
     when b.metric = 'LTV_TO_CAC' then 'LTV to CAC Ratio'
     when b.metric = 'FCF_MARGIN' then 'FCF Margin'
     when b.metric = 'NET_NEW_ARR' then 'Net NEw ARR'
     when b.metric = 'BURN_MULTIPLE' then 'Burn Multiple'
     when b.metric = 'AWS_EXPENSE' then 'Cloud Expense (AWS)'
     when b.metric = 'SOFTWARE_PER_FTE' then 'Software Expense / FTE'
     when b.metric = 'REAL_ESTATE_EXPENSE' then 'Real Estate Expense'
     when b.metric = 'R_AND_D_EXPENSE' then 'R&D Expense'
     when b.metric = 'S_AND_M_EXPENSE' then 'S&M Expense'
     when b.metric = 'G_AND_A_EXPENSE' then 'G&A Expense'
     when b.metric = 'TOTAL_OPEX' then 'Total Operating Expense'
     when b.metric = 'GAAP_REV_PER_FTE' then 'GAAP Revenue / FTE'
     when b.metric = 'COMP_AND_BENEFITS_PER_REV' then 'Compensation & Benefits / GAAP revenue'
     when b.metric = 'ENG_FTE' then 'Engineering FTEs'
     when b.metric = 'SALES_FTE' then 'Sales FTEs'
     when b.metric = 'CX_FTE' then 'Customer Experience FTEs'
     when b.metric = 'MARKETING_FTE' then 'Marketing FTEs'
     when b.metric = 'PRODUCT_FTE' then 'Product FTEs'
     when b.metric = 'FINANCE_FTE' then 'FInance FTEs'
     when b.metric = 'PEOPLE_FTE' then 'People FTEs'
     when b.metric = 'LEGAL_FTE' then 'Legal FTEs'
     else 'other' end as metric_label,
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
to_timestamp(qtr_end_dte) as qtr_end_dte,
metric,
metric_order_by_column,
metric_label,
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
