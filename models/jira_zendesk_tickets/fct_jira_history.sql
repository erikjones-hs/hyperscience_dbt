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
project_key,
project_name,
project_category,
resolution_dte as resolution_dte_raw,
CASE WHEN resolution_dte IS NULL then dateadd(day,31,to_date(current_date())) ELSE to_date(resolution_dte) end as resolution_dte_transformed,
CASE WHEN status in ('Done') then to_date(resolution_dte) else to_date(current_date()) end as resolution_dte_for_calcs,
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
rd.project_key,
rd.project_name,
rd.project_category,
rd.resolution_dte_raw,
rd.resolution_dte_transformed,
rd.resolution_dte_for_calcs,
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

labels as (
select distinct
issue_id,
listagg(distinct lower(value),',') within group (order by lower(value)) as labels
from "FIVETRAN_DATABASE"."JIRA"."ISSUE_MULTISELECT_HISTORY"
where field_id = 'labels'
group by issue_id
order by issue_id
),

fct_tickets_history as (
select distinct
to_timestamp(td.dte) as dte,
td.issue_id,
td.issue_key,
td.parent_id,
td.epic_name,
td.resolution_stage,
td.priority,
td.priority_name,
td.assignee_id,
td.assignee_name,
td.status,
td.status_category_name,
td.creator_name,
td.issue_type_name,
td.project_id,
td.project_key,
td.project_name,
td.project_category,
td.resolution_dte_raw,
to_timestamp(td.resolution_dte_transformed) as resolution_dte_transformed,
to_timestamp(td.resolution_dte_for_calcs) as resolution_dte_for_calcs,
td.created_dte,
td.component,
td.component_is_active,
td.version_name,
td.fix_version_name,
td.feedback_category,
td.customer_name,
l.labels,
CASE WHEN to_date(dte) < to_date(resolution_dte_transformed) then 1 else 0 end as open_ticket_fl,
CASE WHEN to_date(dte) = to_date(resolution_dte_transformed) then 1 else 0 end as closed_ticket_fl,
CASE WHEN to_date(dte) < to_date(resolution_dte_transformed) then 'open' else 'closed' end as ticket_resolution_category,
CASE WHEN to_date(dte) = to_date(created_dte) then 1 else 0 end as created_dte_fl,
CASE WHEN to_date(dte) = to_date(resolution_dte_transformed) then 1 else 0 end as resolved_dte_fl,
CASE WHEN date_trunc(week,to_date(dte)) = date_trunc(week,to_date(created_dte)) then 1 else 0 end as created_week_fl,
CASE WHEN date_trunc(week,to_date(dte)) = date_trunc(week,to_date(resolution_dte_transformed)) then 1 else 0 end as resolved_week_fl,
CASE WHEN date_trunc(month,to_date(dte)) = date_trunc(month,to_date(created_dte)) then 1 else 0 end as created_month_fl,
CASE WHEN date_trunc(month,to_date(dte)) = date_trunc(month,to_date(resolution_dte_transformed)) then 1 else 0 end as resolved_month_fl
from ticket_dates as td
left join labels as l on (td.issue_id = l.issue_id)
order by td.issue_id, dte asc
)

select * from fct_tickets_history