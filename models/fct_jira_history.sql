{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'JIRA'
)
}}

with raw_data as (
select distinct
issue_id,
issue_key,
parent_id,
epic_name,
resolution_stage,
priority,
priority_name,
assignee_id,
assignee_name,
status,
status_category_name,
creator_name,
issue_type_name,
project_id,
project_name,
project_category,
resolution_dte as resolution_dte_raw,
CASE WHEN resolution_dte IS NULL then dateadd(day,1,to_date(current_date())) ELSE to_date(resolution_dte) end as resolution_dte_transformed,
to_date(created_dte) as created_dte,
component,
component_is_active,
version_name,
fix_version_name,
feedback_category,
customer_name
from {{ref('fct_jira_issues_components_versions')}}
),

dates as (
select distinct
dte
from "DEV"."MARTS"."FY_CALENDAR"
),

ticket_dates as (
select distinct
dates.dte,
rd.issue_id,
rd.issue_key,
rd.parent_id,
rd.epic_name,
rd.resolution_stage,
rd.priority,
rd.priority_name,
rd.assignee_id,
rd.assignee_name,
rd.status,
rd.status_category_name,
rd.creator_name,
rd.issue_type_name,
rd.project_id,
rd.project_name,
rd.project_category,
rd.resolution_dte_raw,
rd.resolution_dte_transformed,
rd.created_dte,
rd.component,
rd.component_is_active,
rd.version_name,
rd.fix_version_name,
rd.feedback_category,
rd.customer_name
from raw_data as rd
inner join dates on (dates.dte >= rd.created_dte AND dates.dte <= rd.resolution_dte_transformed)
order by issue_id, dte asc
),

fct_tickets_history as (
select distinct
dte,
issue_id,
issue_key,
parent_id,
epic_name,
resolution_stage,
priority,
priority_name,
assignee_id,
assignee_name,
status,
status_category_name,
creator_name,
issue_type_name,
project_id,
project_name,
project_category,
resolution_dte_raw,
resolution_dte_transformed,
created_dte,
component,
component_is_active,
version_name,
fix_version_name,
feedback_category,
customer_name,
CASE WHEN to_date(dte) < to_date(resolution_dte_transformed) then 1 else 0 end as open_ticket_fl,
CASE WHEN to_date(dte) = to_date(resolution_dte_transformed) then 1 else 0 end as closed_ticket_fl,
CASE WHEN to_date(dte) < to_date(resolution_dte_transformed) then 'open' else 'closed' end as ticket_resolutin_category
from ticket_dates 
order by issue_id, dte asc
)

select * from fct_tickets_history