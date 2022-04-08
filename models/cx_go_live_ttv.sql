{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with raw_data as (
select 
gsid,
current_opportunity_id,
company_id_gsid,
name,
company_id_name,
sales_partner_name,
current_contracted_volume,
current_contract_start_date,
current_contract_end_date,
time_to_value,
CASE WHEN updated_go_live_goal IS NOT NULL then to_timestamp(updated_go_live_goal)
     ELSE to_timestamp(go_live_goal) end as go_live_goal,
to_timestamp(actual_go_live_date) as go_live_date,
CASE	
	WHEN deployment_type = '1I008S7XT4797JAKQFPZJXG7DWR338GS5W1W' THEN 'Private Cloud - Other'
	WHEN deployment_type = '1I00GGM0EV4W0IEG1NJ8T0H4YWVRQCFNYXS8' THEN 'On-Prem'
	WHEN deployment_type = '1I00GGM0EV4W0IEG1NK6U32IK23NNW34WKFQ' THEN 'SaaS'
	WHEN deployment_type = '1I008S7XT4797JAKQFUZ43LBXFGJ19VRC7I0' THEN 'Private Cloud - AWS'
	WHEN deployment_type = '1I008S7XT4797JAKQFUKSBH88BWCPFWX5A93' THEN 'Private Cloud - Azure'
    ELSE 'Other/ Not Specified' END AS deployment_type,
CASE	
	WHEN project_status = '1I0054U9FAKXZ0H26HYQ2B8AFJJ8A1JW7WX0' THEN 'On Hold'
	WHEN project_status = '1I0054U9FAKXZ0H26HKLLXUV37XV8Q99AVXO' THEN 'In Implementation'
	WHEN project_status = '1I0054U9FAKXZ0H26H98WUZH9EWVZV3J28XX' THEN 'Business Segment'
	WHEN project_status = '1I0054U9FAKXZ0H26H4B1MEHBPNQNJAR3SL0' THEN 'Pre Sales'
	WHEN project_status = '1I0054U9FAKXZ0H26HPHO3LUWQGAMTHXXVZ0' THEN 'Inactive'
    ELSE 'Not Started' END AS project_status,
initial_implementation,
to_timestamp(kpi_start_date) as kpi_start_date
from "FIVETRAN_DATABASE"."S3"."GAINSIGHT_RELATIONSHIP"
),

first_go_live as (
select distinct
company_id_name,
to_timestamp(min(go_live_date)) as first_go_live_date
from raw_data
group by company_id_name
),

fy_dates as (
select distinct
dte,
CASE WHEN month in (1,2) then dateadd('year',-1,date_trunc(year,dte)) ELSE date_trunc(year,dte) end as fy_year,
fy_qtr_year,
qtr_end_dte
from "DEV"."MARTS"."FY_CALENDAR"
),

/* Closed Won Opps to set the base for all current customers */
opps as (
select distinct 
sao.opp_id,
sao.opp_name,
sao.account_owner as account_exec,
sao.account_name as account_name,
sao.account_sales_region  as region,
sao.partner_account_name as partner,
sao.account_industry as industry
from "DEV"."SALES"."SALESFORCE_AGG_OPPORTUNITY" as sao
where sao.opp_stage_name in ('Closed Won')
and sao.is_deleted = false
),

account_exec as (
select distinct
company_id_name,
to_timestamp(min(go_live_date)) as first_go_live_date
from raw_data
group by company_id_name
),

fct_monthly_lpis_cx_live as (
select
rd.gsid,
rd.current_opportunity_id,
rd.name,
rd.company_id_name,
opps.industry,
opps.region,
opps.partner,
opps.account_exec,
rd.current_contracted_volume,
rd.current_contract_start_date,
rd.current_contract_end_date,
rd.time_to_value,
rd.go_live_goal,
rd.go_live_date,
rd.deployment_type,
rd.project_status,
rd.kpi_start_date,
rd.initial_implementation,
fd.fy_year,
fd.fy_qtr_year,
fd.qtr_end_dte as fy_qtr_end_dte,
fgl.first_go_live_date
from raw_data as rd 
left join first_go_live as fgl on (rd.company_id_name = fgl.company_id_name)
left join fy_dates as fd on to_date(fd.dte) = to_date(fgl.first_go_live_date)
left join opps on opps.opp_id=rd.current_opportunity_id
order by company_id_name
)

select * from fct_monthly_lpis_cx_live