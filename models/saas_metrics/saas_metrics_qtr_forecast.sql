{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'SAAS_METRICS'
)
}}

with forecast_raw as (
select distinct 
to_date(date) as date,
to_decimal(arr,38,2) as arr,
to_decimal(revenue,38,2) as revenue,
to_decimal(net_dollar_retention,38,2) as net_dollar_retention,
to_decimal(gross_dollar_retention,38,2) as gross_dollar_retention,
to_decimal(net_logo_churn,38,2) as net_logo_churn,
to_decimal(gross_margin,38,2) as gross_margin,
to_decimal(cac_payback_period,38,2) as cac_payback_period,
to_decimal(cash_conversion_score,38,2) as cash_conversion_score,
to_decimal(rule_of_40,38,2) as rule_of_40,
to_decimal(magic_number,38,2) as magic_number,
to_decimal(ltv_to_cac,38,2) as ltv_to_cac,
to_decimal(financing_cash_flow,38,2) as financing_cash_flow,
to_decimal(operating_cash_flow,38,2) as operating_cash_flow,
to_decimal(new_arr,38,2) as new_arr,
to_decimal(upsell_arr,38,2) as upsell_arr,
to_decimal(churn_arr,38,2) as churn_arr,
to_decimal(net_burn,38,2) as net_burn,
to_decimal(aws_expense,38,2) as aws_expense,
to_decimal(software_subscriptions,38,2) as software_subscriptions,
to_decimal(software_engineering,38,2) as software_engineering,
to_decimal(real_estate_expense,38,2) as real_estate_expense,
to_decimal(engineering_cost_of_rev,38,2) as engineering_cost_of_rev,
to_decimal(product_cost_of_rev,38,2) as product_cost_of_rev,
to_decimal(r_d_expense,38,2) as r_d_expense,
to_decimal(cx_cost_of_rev,38,2) as cx_cost_of_rev,
to_decimal(s_m_expense,38,2) as s_m_expense,
to_decimal(g_a_expense,38,2) as g_a_expense,
to_decimal(g_a_stock_comp,38,2) as g_a_stock_comp,
to_decimal(all_dept_expense,38,2) as all_dept_expense,
to_decimal(salary_wage_allocation,38,2) as salary_wage_allocation,
to_decimal(payroll_tax_allocation,38,2) as payroll_tax_allocation,
to_decimal(benefits_allocation,38,2) as benefits_allocation,
to_decimal(exmployer_401_k_allocation,38,2) as exmployer_401_k_allocation,
to_decimal(salary_and_wages,38,2) as salary_and_wages,
to_decimal(payroll_taxes,38,2) as payroll_taxes,
to_decimal(payroll_benefits,38,2) as payroll_benefits,
to_decimal(health_club,38,2) as health_club,
to_decimal(employer_match_401_k,38,2) as employer_match_401_k,
to_decimal(dependent_care_education,38,2) as dependent_care_education,
to_decimal(bonus,38,2) as bonus,
to_decimal(severance,38,2) as severance,
to_decimal(relocation,38,2) as relocation,
to_decimal(temp_labor,38,2) as temp_labor,
to_decimal(eng,38,2) as eng,
to_decimal(new_eng,38,2) as new_eng,
to_decimal(sales,38,2) as sales,
to_decimal(cx,38,2) as cx,
to_decimal(marketing,38,2) as marketing,
to_decimal(product,38,2) as product,
to_decimal(finance,38,2) as finance,
to_decimal(people,38,2) as people,
to_decimal(legal,38,2) as legal,
to_decimal(all_dept,38,2) as all_dept
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FINANCE_FORECAST"
order by date asc
),

forecast_qtr_transform as (
select distinct
date,
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then arr else 0 end as arr,
revenue,
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then net_dollar_retention else 0 end as net_dollar_retention, 
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then gross_dollar_retention else 0 end as gross_dollar_retention,
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then ((100*net_logo_churn) + 100) else 0 end as net_logo_retention,
gross_margin,
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then cac_payback_period else 0 end as cac_payback_period,
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then cash_conversion_score else 0 end as cash_conversion_score, 
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then rule_of_40 else 0 end as rule_of_40,
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then magic_number else 0 end as magic_number,
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then ltv_to_cac else 0 end as ltv_to_cac,
(financing_cash_flow + operating_cash_flow) as fcf_margin_numerator,
(new_arr + upsell_arr + churn_arr) as net_new_arr,
net_burn as burn_multiple_numerator,
aws_expense,
(software_subscriptions + software_engineering) as software_per_fte_numerator,
real_estate_expense, 
(r_d_expense + engineering_cost_of_rev + product_cost_of_rev) as r_and_d_expense,
(s_m_expense + cx_cost_of_rev) as s_and_m_expense,
(g_a_expense - g_a_stock_comp) as g_and_a_expense,
(all_dept_expense - g_a_stock_comp) as total_opex,
(salary_wage_allocation + payroll_tax_allocation + benefits_allocation + exmployer_401_k_allocation + 
 salary_and_wages + payroll_taxes + payroll_benefits + health_club + employer_match_401_k + dependent_care_education + 
 bonus + severance + relocation + temp_labor) as comp_benefits_numerator,
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then (eng + new_eng) else 0 end as eng_fte,
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then sales else 0 end as sales_fte,
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then cx else 0 end as cx_fte,
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then marketing else 0 end as marketing_fte,
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then product else 0 end as product_fte,
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then finance else 0 end as finance_fte,
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then people else 0 end as people_fte,
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then legal else 0 end as legal_fte,
CASE WHEN to_date(date) in ('2023-02-28','2023-05-31','2023-08-31','2023-11-30','2024-02-29','2024-05-31','2024-08-31','2024-11-30','2025-02-28') then all_dept else 0 end as all_dept_fte
from forecast_raw
),

fy_dates as (
select distinct 
dte,
CASE WHEN month in (1,2) then dateadd('year',-1,date_trunc(year,dte)) ELSE date_trunc(year,dte) end as fy_year,
fy_qtr_year,
qtr_end_dte
from "DEV"."MARTS"."FY_CALENDAR"
where to_date(dte) >= '2022-12-31'
and to_date(dte) <= '2025-02-28'
),

/* merging in quarter end dates to be able to aggregate by fiscal quarter */
forecast_transform_dates as (
select distinct
date,
fd.qtr_end_dte,
arr,
revenue,
net_dollar_retention, 
gross_dollar_retention,
net_logo_retention,
gross_margin,
cac_payback_period,
cash_conversion_score,
rule_of_40,
magic_number,
ltv_to_cac,
fcf_margin_numerator,
net_new_arr,
burn_multiple_numerator,
aws_expense,
software_per_fte_numerator,
real_estate_expense, 
r_and_d_expense,
s_and_m_expense,
g_and_a_expense,
total_opex,
comp_benefits_numerator,
eng_fte,
sales_fte,
cx_fte,
marketing_fte,
product_fte,
finance_fte,
people_fte,
legal_fte,
all_dept_fte
from forecast_qtr_transform as bqt
left join fy_dates as fd on (to_date(bqt.date) = to_date(fd.dte)) 
order by bqt.date asc
),

/* aggregating by quarter */
forecast_qtr_aggregate_int as (
select distinct
qtr_end_dte,
sum(arr) as arr,
sum(revenue) as revenue,
sum(net_dollar_retention ) as net_dollar_retention ,
sum(gross_dollar_retention) as gross_dollar_retention,
sum(net_logo_retention) as net_logo_retention,
sum(gross_margin) as gross_margin,
sum(cac_payback_period) as cac_payback_period,
sum(cash_conversion_score) as cash_conversion_score,
sum(rule_of_40) as rule_of_40,
sum(magic_number) as magic_number,
sum(ltv_to_cac) as ltv_to_cac,
sum(fcf_margin_numerator) as fcf_margin_numerator,
sum(net_new_arr) as net_new_arr,
sum(burn_multiple_numerator) as burn_multiple_numerator,
sum(aws_expense) as aws_expense,
sum(software_per_fte_numerator) as software_per_fte_numerator,
sum(real_estate_expense) as real_estate_expense,
sum(r_and_d_expense) as r_and_d_expense,
sum(s_and_m_expense) as s_and_m_expense,
sum(g_and_a_expense) as g_and_a_expense,
sum(total_opex) as total_opex,
sum(comp_benefits_numerator) as comp_benefits_numerator,
sum(eng_fte) as eng_fte,
sum(sales_fte) as sales_fte,
sum(cx_fte) as cx_fte,
sum(marketing_fte) as marketing_fte,
sum(product_fte) as product_fte,
sum(finance_fte) as finance_fte,
sum(people_fte) as people_fte,
sum(legal_fte) as legal_fte,
sum(all_dept_fte) as all_dept_fte
from forecast_transform_dates
group by qtr_end_dte
order by qtr_end_dte asc
),

/* Adding in any ratio calculations */
forecast_qtr_aggregate as (
select distinct
qtr_end_dte,
ARR,
REVENUE,
NET_DOLLAR_RETENTION,
GROSS_DOLLAR_RETENTION,
NET_LOGO_RETENTION,
GROSS_MARGIN,
to_decimal((gross_margin / nullifzero(revenue)),38,2) as gross_margin_percent,
CAC_PAYBACK_PERIOD,
CASH_CONVERSION_SCORE,
RULE_OF_40,
MAGIC_NUMBER,
LTV_TO_CAC,
FCF_MARGIN_NUMERATOR,
to_decimal((fcf_margin_numerator / nullifzero(revenue)),38,2) as fcf_margin,
NET_NEW_ARR,
BURN_MULTIPLE_NUMERATOR,
to_decimal((abs(burn_multiple_numerator) / nullifzero(net_new_arr)),38,2) as burn_multiple,
AWS_EXPENSE,
SOFTWARE_PER_FTE_NUMERATOR,
to_decimal((software_per_fte_numerator / nullifzero(all_dept_fte)),38,2) as software_per_fte,
REAL_ESTATE_EXPENSE,
R_AND_D_EXPENSE,
S_AND_M_EXPENSE,
G_AND_A_EXPENSE,
TOTAL_OPEX,
to_decimal((revenue / nullifzero(all_dept_fte)),38,2) as gaap_rev_per_fte,
to_decimal((comp_benefits_numerator / nullifzero(revenue)),38,2) as comp_and_benefits_per_rev,
ENG_FTE,
SALES_FTE,
CX_FTE,
MARKETING_FTE,
PRODUCT_FTE,
FINANCE_FTE,
PEOPLE_FTE,
LEGAL_FTE,
ALL_DEPT_FTE
from forecast_qtr_aggregate_int
order by qtr_end_dte asc
),

forecast_qtr_pivot as (
select * from forecast_qtr_aggregate
  unpivot(forecast for metric in (ARR, REVENUE, NET_DOLLAR_RETENTION, GROSS_DOLLAR_RETENTION, NET_LOGO_RETENTION,GROSS_MARGIN, GROSS_MARGIN_PERCENT,	
                                CAC_PAYBACK_PERIOD,	CASH_CONVERSION_SCORE, RULE_OF_40, MAGIC_NUMBER, LTV_TO_CAC, FCF_MARGIN_NUMERATOR, fcf_margin,	
                                NET_NEW_ARR, BURN_MULTIPLE_NUMERATOR, burn_multiple, AWS_EXPENSE, SOFTWARE_PER_FTE_NUMERATOR, software_per_fte,	
                                REAL_ESTATE_EXPENSE, R_AND_D_EXPENSE, S_AND_M_EXPENSE, G_AND_A_EXPENSE, TOTAL_OPEX, gaap_rev_per_fte, comp_and_benefits_per_rev,	
                                ENG_FTE, SALES_FTE, CX_FTE, MARKETING_FTE, PRODUCT_FTE,	FINANCE_FTE, PEOPLE_FTE, LEGAL_FTE, ALL_DEPT_FTE))
order by qtr_end_dte asc
)

select * from forecast_qtr_pivot