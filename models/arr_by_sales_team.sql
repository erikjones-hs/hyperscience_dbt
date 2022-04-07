{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with sales_federal_goals as (
select distinct 
to_number(sales_federal_goal) as new_arr,
'Federal' as sales_team,
'Budget' as category
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
where to_date(date) = '2022-03-01'
),

sales_channel_goals as (
select distinct 
to_number(sales_channel_goal) as new_arr,
'Channel' as sales_team,
'Budget' as category
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
where to_date(date) = '2022-03-01'
),

sales_us_east_goals as (
select distinct
to_number(sales_us_east_goal) as new_arr,
'US East' as sales_team,
'Budget' as category
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
where to_date(date) = '2022-03-01'
),

sales_us_west_goals as (
select distinct 
to_number(sales_us_west_goal) as new_arr,
'US West' as sales_team,
'Budget' as category
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
where to_date(date) = '2022-03-01'
),

sales_apac_goals as (
select distinct 
to_number(sales_apac_goal) as new_arr,
'APAC' as sales_team,
'Budget' as category
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
where to_date(date) = '2022-03-01'
),

fct_goals as (
select * from sales_federal_goals
UNION
select * from sales_channel_goals
UNION 
select * from sales_us_east_goals
UNION 
select * from sales_us_west_goals
UNION 
select * from sales_apac_goals
order by sales_team
),

opps as (
select * from {{ ref('fct_arr_opp') }}
),

owner as (
select distinct
opp_id,
opp_name,
account_id,
account_name,
opp_owner_id,
opportunity_owner,
owner_description
from "DBT"."DBT_EJONES"."AGG_OPPORTUNITY_INCREMENTAL"
where to_date(date_ran) = dateadd(day,-1,(to_date(current_date)))
),

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
opp.mrr_change_acct,
opp_category,
opp.customer_category,
opp.revenue_category,
o.opp_owner_id,
o.opportunity_owner,
o.owner_description
from opps as opp
left join owner as o on (opp.opp_id = o.opp_id)
where opp.date_month <= date_trunc('month',to_date(current_date()))
order by opp.date_month asc, opp.opp_id 
),

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
     ELSE 'Other' end as sales_team
from fct_opp_owner 
order by opp_id, date_month asc
),

fy_dates as (
select distinct
dte,
month,
day_of_year,
day_of_qtr,
fy_quarter,
fy_year
from "DEV"."MARTS"."FY_CALENDAR"
),

fct_fy_teams as (
select distinct
foot.date_month,
foot.account_id,
foot.account_name,
foot.mrr_acct,
foot.mrr_change_acct,
foot.customer_category,
foot.revenue_category,
foot.sales_team,
fd.fy_quarter,
fd.fy_year
from fct_opp_owner_teams as foot
left join fy_dates as fd on (to_date(foot.date_month) = date_trunc('month',to_date(fd.dte))) 
order by foot.account_id, foot.date_month asc
),

fy_agg_int as (
select 
date_month as dte,
ZEROIFNULL(sum(mrr_change_acct) over (partition by fy_year, sales_team order by date_month asc rows between unbounded preceding and current row)) as new_arr,
sales_team,
'Actuals' as category
from fct_fy_teams
where revenue_category in ('new','expansion')
order by sales_team, fy_year asc, date_month asc
),

fy_agg as (
select distinct
new_arr,
sales_team,
category
from fy_agg_int
where to_date(dte) >= '2022-03-01'
),

fct_sales_teams_goals_actual_fytd as (
select * from fct_goals
UNION 
select * from fy_agg
order by sales_team, category
)

select * from fct_sales_teams_goals_actual_fytd