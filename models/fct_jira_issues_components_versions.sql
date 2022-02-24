{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'JIRA'
)
}}

with issues as (
select distinct 
i.id as issue_id,
i.key as issue_key,
i.parent_id,
e.name as epic_name,
r.name as resolution_stage,
i.priority as priority,
p.name as priority_name,
i.assignee as assignee_id,
u.name as assignee_name,
s.name as status,
sc.name as status_category_name,
u2.name as creator_name,
it.name as issue_type_name,
i.project as project_id,
proj.key as project_key,
proj.name as project_name,
pc.name as project_category,
to_timestamp(i.resolved) as resolution_dte,
to_timestamp(i.created) as created_dte
from "FIVETRAN_DATABASE"."JIRA"."ISSUE" as i
left join "FIVETRAN_DATABASE"."JIRA"."EPIC" as e on (i.key = e.key)
left join "FIVETRAN_DATABASE"."JIRA"."RESOLUTION" as r on (i.resolution = r.id)
left join "FIVETRAN_DATABASE"."JIRA"."PRIORITY" as p on (i.priority = p.id)
left join "FIVETRAN_DATABASE"."JIRA"."USER" as u on (i.assignee = u.id)
left join "FIVETRAN_DATABASE"."JIRA"."STATUS" as s on (i.status = s.id)
left join "FIVETRAN_DATABASE"."JIRA"."STATUS_CATEGORY" as sc on (s.status_category_id = sc.id)
left join "FIVETRAN_DATABASE"."JIRA"."USER" as u2 on (i.creator = u2.id)
left join "FIVETRAN_DATABASE"."JIRA"."USER" as u3 on (i.reporter = u3.id)
left join "FIVETRAN_DATABASE"."JIRA"."ISSUE_TYPE" as it on (i.issue_type = it.id)
left join "FIVETRAN_DATABASE"."JIRA"."PROJECT" as proj on (i.project = proj.id)
left join "FIVETRAN_DATABASE"."JIRA"."PROJECT_CATEGORY" as pc on (proj.project_category_id = pc.id)
order by i.id desc 
),

components as (
select 
imh.issue_id,
c.name as component_name,
imh.is_active as component_is_active
from "FIVETRAN_DATABASE"."JIRA"."ISSUE_MULTISELECT_HISTORY" as imh
left join "FIVETRAN_DATABASE"."JIRA"."COMPONENT" as c on (imh.value = c.id)
where field_id = 'components'
and imh.value IS NOT NULL
order by imh.issue_id
),

versions as (
select 
imh.issue_id,
v.name as version_name,
imh.is_active as version_is_active
from "FIVETRAN_DATABASE"."JIRA"."ISSUE_MULTISELECT_HISTORY" as imh
left join "FIVETRAN_DATABASE"."JIRA"."VERSION" as v on (imh.value = v.id)
where field_id = 'versions'
and imh.value IS NOT NULL
order by imh.issue_id
),

feedback_category as (
select distinct
imh.issue_id,
fo.name as feedback_category
from "FIVETRAN_DATABASE"."JIRA"."ISSUE_MULTISELECT_HISTORY" as imh
left join "FIVETRAN_DATABASE"."JIRA"."FIELD_OPTION" as fo on (imh.value = fo.id)
where imh.field_id = 'customfield_10669'
order by imh.issue_id
),

customer_name as (
select distinct
imh.issue_id,
fo.name as customer_name
from "FIVETRAN_DATABASE"."JIRA"."ISSUE_MULTISELECT_HISTORY" as imh
left join "FIVETRAN_DATABASE"."JIRA"."FIELD_OPTION" as fo on (imh.value = fo.id)
where imh.field_id = 'customfield_10666'
order by imh.issue_id
),

fct_issue_component_version as (
select distinct 
i.issue_id,
i.issue_key,
i.parent_id,
i.epic_name,
i.resolution_stage,
i.priority,
i.priority_name,
i.assignee_id,
i.assignee_name,
i.status,
i.status_category_name,
i.creator_name,
i.issue_type_name,
i.project_id,
i.project_key,
i.project_name,
i.project_category,
i.resolution_dte,
i.created_dte,
c.component_name as component,
c.component_is_active,
v.version_name,
v.version_is_active,
fc.feedback_category,
cn.customer_name
from issues as i
left join components as c on (i.issue_id = c.issue_id)
left join versions as v on (i.issue_id = v.issue_id)
left join feedback_category as fc on (i.issue_id = fc.issue_id)
left join customer_name as cn on (i.issue_id = cn.issue_id)
order by i.issue_id desc
)

select * from fct_issue_component_version 


