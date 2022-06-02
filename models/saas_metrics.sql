{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'SAAS_METRICS'
)
}}

with raw_data as (
select *,
to_date(date) as dte
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."SAAS_METRICS"
order by dte asc
),

fy_dates as (
select distinct 
dte,
CASE WHEN month in (1,2) then dateadd('year',-1,date_trunc(year,dte)) ELSE date_trunc(year,dte) end as fy_year,
fy_qtr_year,
qtr_end_dte
from "DEV"."MARTS"."FY_CALENDAR"
where to_date(dte) >= '2020-01-01'
and to_date(dte) <= '2024-02-29'
),

saas_metrics_revenue_int as (
select distinct
to_timestamp(rd.dte) as dte,
rd.total_arr,
((rd.total_arr - lag(rd.total_arr,12,0) over (order by rd.dte asc)) / NULLIFZERO(lag(rd.total_arr,12,0) over (order by rd.dte asc))) as arr_percent_growth,
rd.revenue,
sum(rd.revenue) over (partition by fd.fy_year order by rd.dte asc rows between unbounded preceding and current row) as revenue_fy,
rd.net_dollar_retention,
rd.gross_dollar_retention,
rd.gross_profit,
sum(rd.gross_profit) over (partition by fd.fy_year order by rd.dte asc rows between unbounded preceding and current row) as gross_profit_fy,
(gross_profit_fy / NULLIFZERO(revenue_fy)) as gross_margin,
rd.net_logo_churn,
rd.cac_payback_months,
rd.cash_conversion_score,
rd.net_burn,
sum(rd.net_burn) over (partition by fd.fy_year order by rd.dte asc rows between unbounded preceding and current row) as net_burn_fy,
(net_burn_fy / NULLIFZERO(revenue_fy)) as fcf_margin,
rd.rule_of_40,
rd.ltv_to_cac,
rd.arr_growth_percent_goal_25,
rd.arr_growth_percent_goal_median,
rd.arr_growth_percent_goal_75,
rd.revenue_growth_percent_25,
rd.revenue_growth_percent_median,
rd.revenue_growth_percent_75,
rd.net_dollar_retention_25,
rd.net_dollar_retention_median,
rd.net_dollar_retention_75,
rd.gross_dollar_retention_25,
rd.gross_dollar_retention_median,
rd.gross_dollar_retention_75,
rd.gross_margin_25,
rd.gross_margin_median,
rd.gross_margin_75,
rd.net_logo_churn_25,
rd.net_logo_churn_median,
rd.net_logo_churn_75,
rd.cac_payback_median,
rd.cash_conversion_score_25,
rd.cash_conversion_score_median,
rd.cash_conversion_score_75,
rd.fcf_25,
rd.fcf_median,
rd.fcf_75,
rd.rule_of_40_median
from raw_data as rd
left join fy_dates as fd on (rd.dte = fd.dte)
order by dte asc
),

saas_metrics_revenue as (
select *,
((revenue_fy - lag(revenue_fy,12,0) over (order by dte asc)) / NULLIFZERO(lag(revenue_fy,12,0) over (order by dte asc))) as revenue_percent_growth
from saas_metrics_revenue_int
order by dte asc
)

select * from saas_metrics_revenue