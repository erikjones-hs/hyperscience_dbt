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

sales_actuals as (
select distinct
date_trunc('month',to_date(close_dte)) as close_month,
sales_team,
sum(mrr_change_acct) as new_arr_actuals
from fct_opp_owner_teams
where revenue_category in ('new','expansion')
and close_month <= last_day(date_trunc('month',to_date(current_date())))
group by close_month, sales_team
order by close_month asc, sales_team 
),

actuals_running_total_int1 as (
select distinct
date_trunc('month',fd.dte) as dte,
fd.qtr_end_dte,
sa.close_month,
sa.sales_team,
sa.new_arr_actuals as new_arr_actuals,
coalesce(sa1.new_arr_actuals,0) as actuals
from fy_dates as fd
inner join sales_actuals as sa on (date_trunc('month',fd.dte) >= '2022-03-01' AND date_trunc('month',fd.dte) <= '2023-02-08')
left join sales_actuals as sa1 on (sa1.close_month = date_trunc('month',fd.dte) AND sa1.sales_team = sa.sales_team)
order by sales_team, dte asc
),

actuals_running_total_int2 as (
select distinct
dte,
qtr_end_dte,
sales_team,
actuals
from actuals_running_total_int1
order by sales_team, dte asc
),

actuals_running_total_int3 as (
select distinct
dte,
qtr_end_dte,
sales_team,
sum(actuals) over (partition by sales_team, qtr_end_dte order by dte asc rows between unbounded preceding and current row) as actuals_running_total_fq
from actuals_running_total_int2
where date_trunc('month',dte) <= to_date(date_trunc('month',current_date()))
order by qtr_end_dte asc, dte asc
),

actuals_running_total_fq as (
select distinct
dte,
qtr_end_Dte,
sales_team,
actuals_running_total_fq,
lag(actuals_running_total_fq,1,0) over (partition by sales_team order by dte asc) as prev_month_running_total
from actuals_running_total_int3 
order by qtr_end_dte asc, dte asc
),

sales_budget_running_total_int as (
select distinct
sb.dte as dte_month,
sb.sales_team,
sb.budget,
fd.fy_year,
fd.fy_qtr_year,
fd.qtr_end_dte,
fd.dte
from sales_budget as sb 
right join fy_dates as fd on (sb.dte = fd.dte)
order by dte asc
),

sales_budget_running_total_fq as (
select distinct
dte_month,
qtr_end_dte,
sales_team,
sum(budget) over (partition by sales_team, qtr_end_dte order by dte_month asc rows between unbounded preceding and current row) as sales_budget_running_total_fq
from sales_budget_running_total_int
where dte_month IS NOT NULL
order by qtr_end_dte asc, dte_month asc
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
--from "DEV"."SALES"."SALESFORCE_AGG_OPPORTUNITY"
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

commit_running_total_int as (
select distinct 
ca.close_month,
ca.sales_team,
ca.arr_committed,
fd.fy_year,
fd.fy_qtr_year,
fd.qtr_end_dte,
fd.dte
from commit_agg as ca
right join fy_dates as fd on (ca.close_month = fd.dte)
order by dte asc
),

commit_running_total_fq as (
select distinct
close_month,
qtr_end_dte,
sales_team,
sum(arr_committed) over (partition by sales_team, qtr_end_dte order by close_month asc rows between unbounded preceding and current row) as arr_committed_running_total_fq
from commit_running_total_int
where close_month IS NOT NULL
order by qtr_end_dte asc, close_month asc
),

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

fct_forecast_int as (
select distinct
sb.dte,
sb.sales_team,
sb.budget,
sbrtf.sales_budget_running_total_fq as sales_budget_running_total,
CASE WHEN sa.new_arr_actuals IS NULL then 0 else sa.new_arr_actuals end as new_arr_actuals,
CASE WHEN artf.actuals_running_total_fq IS NULL then 0 else artf.actuals_running_total_fq end as actuals_running_total,
CASE WHEN artf.prev_month_running_total IS NULL then 0 else artf.prev_month_running_total end as prev_month_running_total,
CASE WHEN ca.arr_committed IS NULL then 0 else ca.arr_committed end as arr_committed,
CASE WHEN bca.arr_best_case IS NULL then 0 else bca.arr_best_case end as arr_best_case,
ZEROIFNULL(crtf.arr_committed_running_total_fq) as arr_committed_running_total_fq 
from sales_budget as sb
left join sales_budget_running_total_fq as sbrtf on (sb.dte = sbrtf.dte_month AND sb.sales_team = sbrtf.sales_team)
left join sales_actuals as sa on (sb.dte = last_day(sa.close_month) AND sb.sales_team = sa.sales_team)
left join actuals_running_total_fq as artf on (sb.dte = last_day(artf.dte) AND sb.sales_team = artf.sales_team)  
left join best_case_agg as bca on (sb.dte = bca.close_month AND sb.sales_team = bca.sales_team) 
left join commit_agg as ca on (sb.dte = ca.close_month AND sb.sales_team = ca.sales_team)
left join commit_running_total_fq as crtf on (sb.dte = crtf.close_month AND sb.sales_team = crtf.sales_team)
order by sb.dte asc, sb.sales_team
),

fct_forecast as (
select distinct
to_timestamp(dte) as dte,
sales_team,
budget,
sales_budget_running_total,
new_arr_actuals,
actuals_running_total,
prev_month_running_total,
arr_committed * .75 as arr_low,
arr_committed,
arr_best_case,
(.4 * arr_best_case) + arr_committed as high_best_case,
(sales_budget_running_total - prev_month_running_total) as forecast_plan,
arr_committed_running_total_fq as arr_committed_running_total,
(.4 * arr_best_case) + arr_committed_running_total_fq as best_case_high_running_total
from fct_forecast_int 
order by dte asc, sales_team
)

select * from fct_forecast