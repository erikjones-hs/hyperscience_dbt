{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'ARR_FORECAST'
)
}}

/* Pulling in the BOY budget numbers by sales teams */
with sales_budget_int as (
select distinct
to_date(date) as dte,
us_west,
us_east,
federal,
channel,
apac,
emea,
other
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_SALES_BUDGET_BY_REGION"
order by dte asc
),

sales_budget_pivot as (
select * from sales_budget_int
  unpivot(budget for sales_team in (us_west, us_east, federal, channel, apac, emea, other))
order by dte asc
),

sales_budget as (
select distinct
dte,
CASE WHEN SALES_TEAM = 'US_EAST' then 'US East'
     WHEN SALES_TEAM = 'US_WEST' then 'US West'
     WHEN SALES_TEAM = 'FEDERAL' then 'Federal'
     WHEN SALES_TEAM = 'CHANNEL' then 'Channel'
     WHEN SALES_TEAM = 'EMEA' then 'EMEA'
     WHEN sales_team = 'OTHER' then 'Other'
     ELSE sales_team end as sales_team,
budget
from sales_budget_pivot
order by dte asc
),

/* Closed Won Opportunities for computing Sales Actuals by team */
/* Using FCT_ARR_OPP SO IT MATCHES WITH OUR KPI DASHBOARD */
opps as (
select * from {{ ref('fct_arr_opp') }}
),

/* Need to pull in opportunity owner to assign to sales team */
owner as (
select distinct
opp_id,
opp_name,
account_id,
account_name,
opp_owner_id,
opportunity_owner,
owner_description,
opp_closed_won_dte
from {{ ref('agg_opportunity_incremental') }}
where to_date(date_ran) = dateadd(day,-1,(to_date(current_date)))
),

/* Combining Opps and Opp Owner */
fct_opp_owner as (
select 
opp.date_month,
opp.account_id,
opp.account_name,
opp.opp_id,
opp.opp_name,
opp.start_dte_month,
opp.start_dte,
opp.end_dte_month,
opp.end_dte,
opp.mrr,
opp.mrr_change,
opp.is_active,
opp.mrr_acct,
CASE WHEN opp.mrr_change = 0 and opp.opp_category = 'recurring' then 0 else opp.mrr_change_acct end as mrr_change_acct,
opp_category,
opp.customer_category,
opp.revenue_category,
o.opp_owner_id,
o.opportunity_owner,
o.owner_description,
o.opp_closed_won_dte
from opps as opp
left join owner as o on (opp.opp_id = o.opp_id)
where opp.date_month < '2023-03-01'
and opp.date_month >= '2022-03-01'
order by opp.date_month asc, opp.opp_id 
),

/* Assigning Sales teams */
fct_opp_owner_teams as (
select distinct 
date_month,
account_id,
account_name,
opp_id,
opp_name,
start_dte_month,
start_dte,
end_dte_month,
end_dte,
mrr,
mrr_change,
is_active,
mrr_acct,
mrr_change_acct,
opp_category,
customer_category,
revenue_category,
opp_closed_won_dte,
opp_owner_id,
opportunity_owner,
owner_description,
CASE WHEN owner_description in ('EMEA Account Executive','EMEA VP','EMEA Sales Outreach','EMEA Central Europe - Account Executive',
                                'EMEA Central Europe - Channel Sales','EMEA Central Europe - Regional Director','EMEA North Europe - Account Executive',
                                'EMEA North Europe - Channel Sales','EMEA North Europe - Regional Director','EMEA - Sales AVP') then 'EMEA'
     WHEN owner_description in ('Account Manager','US Commercial - Account Executive','US Commercial - Regional Director') then 'Commercial'
     WHEN owner_description in ('ANZ - Channel Sales','ANZ - Regional Director') then 'APAC'
     WHEN owner_description in ('US Federal','US Federal - Account Executive','US SLED - Regional Director','US SLED - Account Executive','Global Federal - Account Executive',
                                'Global Public Sector - AVP','Global Federal - Channel Sales') then 'Federal'
     WHEN owner_description in ('US Account Executive - Tri-State','US Director - Tri-State','US AVP - Tri-State','US East - Regional Director','US East - Account Executive') then 'US East'
     WHEN owner_description in ('US West - Account Executive','US West - Regional Director') then 'US West'
     WHEN owner_description in ('US - Channel Sales','Global GSI - Channel Sales','Global GSP - Channel Sales') then 'Channel'
     ELSE 'Other' end as sales_team,
--CASE WHEN opp_closed_won_dte < start_dte then opp_closed_won_dte else start_dte end as close_dte
start_dte as close_dte
from fct_opp_owner 
order by opp_id, date_month asc
),

fy_dates as (
select distinct 
dte,
CASE WHEN month in (1,2) then dateadd('year',-1,date_trunc(year,dte)) ELSE date_trunc(year,dte) end as fy_year,
fy_qtr_year,
qtr_end_dte
from "DEV"."MARTS"."FY_CALENDAR"
where to_date(dte) >= '2022-03-01'
and to_date(dte) <= '2023-02-28'
),

/* Aggregating Actuals by Sales Team and Month */
sales_actuals as (
select distinct
date_trunc('month',to_date(close_dte)) as close_month,
sales_team,
sum(mrr_change_acct) as new_arr_actuals
from fct_opp_owner_teams
where revenue_category in ('new','expansion')
and (close_month <= last_day(date_trunc('month',to_date(current_date()))) or opp_id = '0061R000016jsHbQAI')
group by close_month, sales_team
order by close_month asc, sales_team 
),

pipeline as (
select distinct
account_id,
account_name,
opp_id,
opp_name,
opp_commit_status,
opportunity_owner,
owner_description,
CASE WHEN owner_description in ('EMEA Account Executive','EMEA VP','EMEA Sales Outreach','EMEA Central Europe - Account Executive',
                                'EMEA Central Europe - Channel Sales','EMEA Central Europe - Regional Director','EMEA North Europe - Account Executive',
                                'EMEA North Europe - Channel Sales','EMEA North Europe - Regional Director','EMEA - Sales AVP') then 'EMEA'
     WHEN owner_description in ('Account Manager','US Commercial - Account Executive','US Commercial - Regional Director') then 'Commercial'
     WHEN owner_description in ('ANZ - Channel Sales','ANZ - Regional Director') then 'APAC'
     WHEN owner_description in ('US Federal','US Federal - Account Executive','US SLED - Regional Director','US SLED - Account Executive','Global Federal - Account Executive',
                                'Global Public Sector - AVP','Global Federal - Channel Sales') then 'Federal'
     WHEN owner_description in ('US Account Executive - Tri-State','US Director - Tri-State','US AVP - Tri-State','US East - Regional Director','US East - Account Executive') then 'US East'
     WHEN owner_description in ('US West - Account Executive','US West - Regional Director') then 'US West'
     WHEN owner_description in ('US - Channel Sales','Global GSI - Channel Sales','Global GSP - Channel Sales') then 'Channel'
     ELSE 'Other' end as sales_team,
CASE WHEN opp_id = '0061R000014vnNlQAI' then 150000
     WHEN opp_id = '0061R000014yHlcQAE' then 368750
     else opp_net_new_arr end as opp_net_new_arr,
opp_stage_name,
opp_arr,
opp_close_dte
from {{ ref('agg_opportunity_incremental') }}
where to_date(date_ran) = dateadd(day,-1,(to_date(current_date)))
and opp_stage_name not in ('Opp DQed','Closed Won')
and opp_revenue_type not in ('Renewal','License Overage')
and opportunity_owner not in ('Eli Berman')
and opp_commit_status in ('Committed','Best Case','Visible Opportunity','Pipeline')
and opp_name not in ('Mutual of Omaha-2020-21 auto renew')
and to_date(opp_close_dte) >= '2021-03-01'
and to_date(opp_close_dte) <= '2023-02-28'
),

/* Aggregating Committed Pipeline Dollars by Sales team */
commit_agg as (
select distinct
last_day(date_trunc('month',opp_close_dte)) as close_month,
sales_team,
sum(opp_net_new_arr) as arr_committed
from pipeline
where opp_commit_status in ('Committed')  
group by close_month, sales_team
order by close_month, sales_team
),

/* Aggregating Best Case Pipeline Dollars by Sales Team */
best_case_agg as (
select distinct 
last_day(date_trunc('month',opp_close_dte)) as close_month,
sales_team,
sum(opp_net_new_arr) as arr_best_case
from pipeline
where opp_commit_status in ('Best Case')  
group by close_month, sales_team
order by close_month, sales_team
),

/* Combining Budget and Actuals */
fct_budget_actuals as (
select distinct
sb.dte,
fd.qtr_end_dte,
sb.sales_team,
sb.budget,
ZEROIFNULL(sa.new_arr_actuals) as new_arr_actuals,
ZEROIFNULL(ca.arr_committed) as arr_committed,
ZEROIFNULL(bca.arr_best_case) as arr_best_case
from sales_budget as sb
left join sales_actuals as sa on (sb.dte = last_day(sa.close_month) AND sb.sales_team = sa.sales_team)
left join best_case_agg as bca on (sb.dte = bca.close_month AND sb.sales_team = bca.sales_team) 
left join commit_agg as ca on (sb.dte = ca.close_month AND sb.sales_team = ca.sales_team)
left join fy_dates as fd on (to_date(sb.dte) = to_date(fd.dte))
order by sb.dte asc, sb.sales_team
),

/* Calculating Variance from Budget and Actuals */
fct_budget_variance as (
select distinct
dte,
qtr_end_dte,
sales_team,
budget,
new_arr_actuals,
CASE WHEN to_date(dte) >= date_trunc('month',to_date(current_date())) then 0 else (budget - new_arr_actuals) end as budget_variance,
sum(budget_variance) over (partition by sales_team order by dte asc rows between unbounded preceding and current row) as budget_variance_running_total,
datediff(month,to_date(dte), qtr_end_dte) + 1 as num_months_to_end_of_qtr,
arr_committed * .75 as arr_low,
arr_committed,
arr_best_case,
(arr_best_case + arr_committed) as arr_high
from fct_budget_actuals
order by dte, sales_team
),

/* Calculating Budget Variance Rollover */
rollover_int as (
select distinct
dte,
sales_team,
CASE WHEN to_date(dte) = last_day(date_trunc('month', to_date(current_date()))) then (budget_variance_running_total / num_months_to_end_of_qtr) else NULL end as rollover_monthly_int
from fct_budget_variance
order by dte asc                                 
),

/* Deriving only current month rollover */
current_rollover as (
select distinct
dte,
sales_team,
last_value(rollover_monthly_int ignore nulls) over (partition by sales_team order by dte asc) as rollover_current_month
from rollover_int
order by dte asc, sales_team
),

/* Pulling current QTR date */
current_qtr_int as (
select distinct
dte,
CASE WHEN to_date(dte) = last_day(date_trunc('month',to_date(current_date()))) then qtr_end_dte else NULL end as qtr_end_dte
from fct_budget_variance
),

current_qtr as (
select 
dte,
last_value(qtr_end_dte ignore nulls) over (order by dte asc) as current_qtr
from current_qtr_int
order by dte asc
),

/* Combining Budget, Actuals, Variance and Rollover */
fct_budget_variance_rollover as (
select distinct
fbv.dte,
fbv.qtr_end_dte,
cq.current_qtr,
fbv.sales_team,
fbv.budget,
fbv.new_arr_actuals,
fbv.budget_variance,
fbv.budget_variance_running_total,
fbv.num_months_to_end_of_qtr,
cr.rollover_current_month,
fbv.arr_low,
fbv.arr_committed,
fbv.arr_best_case,
fbv.arr_high
from fct_budget_variance as fbv
left join current_rollover as cr on (to_date(fbv.dte) = to_date (cr.dte) AND fbv.sales_team = cr.sales_team)
left join current_qtr as cq on (to_date(fbv.dte) = to_date(cq.dte))
order by fbv.dte asc, fbv.sales_team
),

/* Calculating Forecast Plan from rollover */
fct_budget_variance_forecast as (
select distinct 
dte,
qtr_end_dte,
current_qtr,
sales_team,
budget,
new_arr_actuals,
budget_variance,
budget_variance_running_total,
num_months_to_end_of_qtr,
rollover_current_month,
CASE WHEN qtr_end_dte = current_qtr then (budget + rollover_current_month) else budget end as forecast_plan,
arr_low,
arr_committed,
arr_best_case,
arr_high
from fct_budget_variance_rollover
order by dte, sales_team
)

select * from fct_budget_variance_forecast
