{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'CX'
)
}}

with assignee_updated as (
SELECT
ticket_id,
dateadd(hour,-4,MAX(updated)) AS assignee_updated_at
FROM "FIVETRAN_DATABASE"."ZENDESK"."TICKET_FIELD_HISTORY"
WHERE field_name = 'assignee_id' 
GROUP BY ticket_id
),

requestor_updated as ( 
SELECT
ticket_id,
dateadd(hour,-4,MAX(updated)) AS requester_updated_at
FROM "FIVETRAN_DATABASE"."ZENDESK"."TICKET_FIELD_HISTORY"
WHERE field_name = 'requester_id' 
GROUP BY ticket_id
),

status_updated as (
SELECT
ticket_id,
dateadd(hour,-4,MAX(updated)) AS status_updated_at
FROM "FIVETRAN_DATABASE"."ZENDESK"."TICKET_FIELD_HISTORY"
WHERE field_name = 'status' 
GROUP BY ticket_id
),

initially_assigned as (
SELECT
ticket_id,
dateadd(hour,-4,MIN(updated)) AS initially_assigned_at
FROM "FIVETRAN_DATABASE"."ZENDESK"."TICKET_FIELD_HISTORY"
WHERE field_name = 'assignee_id' 
GROUP BY ticket_id
),

solved_dte as (
SELECT
ticket_id,
dateadd(hour,-4,MAX(updated)) AS solved_at
FROM "FIVETRAN_DATABASE"."ZENDESK"."TICKET_FIELD_HISTORY"
WHERE value = 'solved' 
GROUP BY ticket_id
),

last_update as (
SELECT
ticket_id,
dateadd(hour,-4,MAX(updated)) AS last_updated_at
FROM "FIVETRAN_DATABASE"."ZENDESK"."TICKET_FIELD_HISTORY"
GROUP BY ticket_id
),

num_groups as (
SELECT
ticket_id,
COUNT(distinct value) AS group_stations
FROM "FIVETRAN_DATABASE"."ZENDESK"."TICKET_FIELD_HISTORY"
WHERE field_name = 'group_id' 
GROUP BY ticket_id
),

num_assignees as (
SELECT
ticket_id,
COUNT(distinct value) AS assignee_stations
FROM "FIVETRAN_DATABASE"."ZENDESK"."TICKET_FIELD_HISTORY"
WHERE field_name = 'assignee_id' 
GROUP BY ticket_id
),

first_reply_time as (
select distinct
tc.ticket_id,
dateadd(hour,-4,min(tc.created)) as first_reply_time
from "FIVETRAN_DATABASE"."ZENDESK"."TICKET_COMMENT" as tc
left join "FIVETRAN_DATABASE"."ZENDESK"."USER" as u on (tc.user_id = u.id)
where u.role not in ('end-user')
group by 1
order by 1 desc
),

ticket as (
select distinct 
tick.id as ticket_id,
tick.via_channel as ticket_via_channel,
tick.via_source_from_address as ticket_source_from_address,
tick.via_source_to_address as ticket_source_to_address,
dateadd(hour,-4,tick.created_at) as ticket_created_at,
dateadd(hour,-4,tick.updated_at) as ticket_updated_at,
tick.type as ticket_type,
lower(tick.subject) as ticket_subject,
tick.description as ticket_description,
tick.priority as ticket_default_priority,
tick.status as ticket_status,
tick.recipient as ticket_recipient,
tick.custom_priority_customer_ as ticket_custom_priority_customer,
tick.CUSTOM_TOTAL_TIME_SPENT_SEC_ as ticket_total_time_spent,
tick.CUSTOM_TIME_SPENT_LAST_UPDATE_SEC_ as ticket_time_spent_since_last_update,
tick.custom_customer_name as ticket_custom_customer_name,
tick.custom_project as ticket_custom_project_fl,
tick.custom_problem_source_internal_ as ticket_problem_source,
tick.custom_infrastructure_component as infrastructure_component,
tick.custom_customer_type as ticket_custom_customer_type,
tick.custom_resolution_code as ticket_resolution_code,
tick.custom_environment as ticket_custom_environment,
tick.custom_jira_ticket_id_don_t_touch_ as ticket_jira_id,
tick.custom_problem_codes_customer_submitted_ as ticket_problem_codes,
tick.custom_severity as ticket_severity,
tick.custom_major_version as ticket_major_version,
tick.custom_hotfix as ticket_hotfix,
tick.custom_duplicate_of_ticket as ticket_duplicate_fl,
tick.custom_activity as ticket_activity,
tick.organization_id,
tick.group_id,
tick.requester_id,
tick.submitter_id,
tick.assignee_id
from "FIVETRAN_DATABASE"."ZENDESK"."TICKET" as tick 
order by tick.id
),

merged_ticket_ids as (
select distinct 
id,
mti.value as merged_ticket_id
from "FIVETRAN_DATABASE"."ZENDESK"."TICKET", lateral flatten(input => parse_json(merged_ticket_ids)) as mti
order by id, mti.value
),

agg_ticket as (
select distinct
tick.*,
CASE WHEN ticket_custom_priority_customer = 'p1' then 1 else 0 end as is_p1_fl,
CASE WHEN ticket_custom_priority_customer = 'p2' then 1 else 0 end as is_p2_fl,
CASE WHEN ticket_custom_priority_customer = 'p3' then 1 else 0 end as is_p3_fl,
grp.name as group_name,
CASE WHEN grp.name = 'TSE' then 1 else 0 end as is_tse_fl,
CASE WHEN grp.name = 'Level 2 Support' then 1 else 0 end as is_level_2_support_fl,
CASE WHEN grp.name = 'Gainsight' then 1 else 0 end as is_gainsight_fl,
CASE WHEN grp.name = 'CSM' then 1 else 0 end as is_csm_fl,
CASE WHEN grp.name = 'Implementation' then 1 else 0 end as is_implementation_fl,
CASE WHEN grp.name = 'Support' then 1 else 0 end as is_support_fl,
org.name as organization_name,
CASE WHEN nullif(tick.ticket_custom_customer_name,'') IS NULL THEN org.name else tick.ticket_custom_customer_name end as customer_name, 
org.custom_csm as org_csm,
org.custom_global_id as org_global_id,
org.custom_organization_type as org_type,
org.custom_customer_lifecycle as org_customer_lifecycle,
au.assignee_updated_at,
ru.requester_updated_at,
su.status_updated_at,
ia.initially_assigned_at,
sd.solved_at,
lu.last_updated_at,
frt.first_reply_time,
timestampdiff(minute,tick.ticket_created_at,frt.first_reply_time) as time_to_first_reply_minutes,
ng.group_stations,
na.assignee_stations,
usr.name as assignee_name,
usr.email as assignee_email,
usr1.name as requester_name,
usr1.email as requester_email,
usr2.name as submitter_name,
usr2.email as submitter_email
from ticket as tick
left join assignee_updated as au on (tick.ticket_id = au.ticket_id) 
left join requestor_updated as ru on (tick.ticket_id = ru.ticket_id) 
left join status_updated as su on (tick.ticket_id = su.ticket_id) 
left join initially_assigned as ia on (tick.ticket_id = ia.ticket_id)
left join solved_dte as sd on (tick.ticket_id = sd.ticket_id) 
left join last_update as lu on (tick.ticket_id = lu.ticket_id)
left join num_groups as ng on (tick.ticket_id = ng.ticket_id)
left join num_assignees as na on (tick.ticket_id = na.ticket_id)
left join first_reply_time as frt on (tick.ticket_id = frt.ticket_id)
left join "FIVETRAN_DATABASE"."ZENDESK"."GROUP" as grp on (tick.group_id = grp.id)
left join "FIVETRAN_DATABASE"."ZENDESK"."ORGANIZATION" as org on (tick.organization_id = org.id)
left join "FIVETRAN_DATABASE"."ZENDESK"."USER" as usr on to_char(tick.assignee_id) = to_char(usr.id)
left join "FIVETRAN_DATABASE"."ZENDESK"."USER" as usr1 on to_char(tick.requester_id) = to_char(usr1.id)
left join "FIVETRAN_DATABASE"."ZENDESK"."USER" as usr2 on to_char(tick.submitter_id) = to_char(usr2.id)
where tick.ticket_id not in (select merged_ticket_id from merged_ticket_ids)
order by tick.ticket_id
)

select * from agg_ticket order by ticket_created_at desc


