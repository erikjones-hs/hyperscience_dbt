{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'PRODUCT_ANALYTICS'
)
}}

with pageviews as (
select distinct
user_id,
event_id,
session_id,
time,
platform,
device_type,
country,
region,
city,
ip,
browser,
domain,
split_part(domain,'.',0) as customer_int,
CASE WHEN split_part(domain,'.',0) = 'benefitmall-prod' then 'Benefit Mall'
     WHEN split_part(domain,'.',0) = 'kovack-prod' then 'Kovack'
     WHEN split_part(domain,'.',0) = 'promomash-prod' then 'Promomash'
     WHEN split_part(domain,'.',0) = 'transflo-prod' then 'Transflo'
     ELSE 'other' end as customer,
path,
title,
CASE WHEN split_part(path,'/',2) = 'document' then 'view_document'
     WHEN split_part(path,'/',2) = 'flows' and split_part(path,'/',3) = '' then 'flows'
     WHEN split_part(path,'/',2) = 'flows' and split_part(path,'/',4) = 'edit' then 'edit_flow'
     WHEN split_part(path,'/',2) = 'layout_variations' and (split_part(path,'/',4) = '' or split_part(path,'/',4) = 'view')  then 'view_layout_variation'
     WHEN split_part(path,'/',2) = 'layout_variations' and split_part(path,'/',4) = 'edit' then 'edit_layout_variation'
     WHEN split_part(path,'/',2) = 'reports' and split_part(path,'/',3) = 'overview' then 'reports_overview'
     WHEN split_part(path,'/',2) = 'reports' and split_part(path,'/',3) = 'usage' then 'reports_usage'
     WHEN split_part(path,'/',2) = 'reports' and split_part(path,'/',3) = 'accuracy' then 'reports_accuracy'
     WHEN split_part(path,'/',2) = 'reports' and split_part(path,'/',3) = '' then 'reports'
     WHEN split_part(path,'/',2) = 'submissions' and split_part(path,'/',3) = 'potential-layouts' then 'view_potential_layout'
     WHEN split_part(path,'/',2) = 'submissions' and split_part(path,'/',4) = '' then 'submission'
     WHEN split_part(path,'/',2) = 'supervision' and split_part(path,'/',4) = '' then 'supervision_task'
     WHEN split_part(path,'/',2) = 'supervision' and split_part(path,'/',3) = 'submission' then 'supervision_submission'
     WHEN split_part(path,'/',2) = 'supervision' and split_part(path,'/',3) = 'task_queue' then 'supervision_task'
     WHEN split_part(path,'/',2) = 'supervision' and split_part(path,'/',3) = 'document' then 'supervision_document'
     WHEN split_part(path,'/',2) = 'tasks' and split_part(path,'/',3) = '' then 'view_task'
     WHEN split_part(path,'/',2) = 'tasks' and split_part(path,'/',3) = 'overview' then 'task_overview'
     WHEN split_part(path,'/',2) = 'tasks' and split_part(path,'/',3) = 'queue' then 'task_queue'
     WHEN split_part(path,'/',2) = 'users' then 'view_users'
     WHEN split_part(path,'/',2) = 'administration' and split_part(path,'/',4) = '' then 'view_admin'
     WHEN split_part(path,'/',2) = 'administration' and split_part(path,'/',3) = 'connection-logs' then 'admin_conection_logs'
     WHEN split_part(path,'/',2) = 'administration' and split_part(path,'/',3) = 'settings' then 'admin_conection_logs'
     WHEN split_part(path,'/',2) = 'administration' and split_part(path,'/',3) = 'system-health' then 'admin_system_health'
     WHEN split_part(path,'/',2) = 'administration' and split_part(path,'/',3) = 'flows' then 'admin_flows'
     WHEN split_part(path,'/',2) = 'administration' and split_part(path,'/',3) = 'import-export' then 'admin_import_export'
     WHEN split_part(path,'/',2) = 'administration' and split_part(path,'/',3) = 'trainer' then 'admin_trainer'
     WHEN split_part(path,'/',2) = 'layouts' and split_part(path,'/',4) = '' then 'view_layouts'
     WHEN split_part(path,'/',2) = 'layouts' and split_part(path,'/',3) = 'library' then 'layouts_library'
     WHEN split_part(path,'/',2) = 'layouts' and split_part(path,'/',3) = 'models' and split_part(path,'/',4) = '' then 'layouts_models'
     WHEN split_part(path,'/',2) = 'layouts' and split_part(path,'/',3) = 'models' and split_part(path,'/',4) != '' and split_part(path,'/',5) = '' then 'layouts_models'
     WHEN split_part(path,'/',2) = 'layouts' and split_part(path,'/',3) = 'models' and split_part(path,'/',4) != '' and split_part(path,'/',5) != '' then 'layouts_model_details'
     WHEN split_part(path,'/',2) = 'layouts' and split_part(path,'/',3) = 'models' and split_part(path,'/',4) = 'classification' then 'layouts_classification_model_management'
     WHEN split_part(path,'/',2) = 'layouts' and split_part(path,'/',3) = 'releases' and split_part(path,'/',4) = '' then 'layouts_releases'
     WHEN split_part(path,'/',2) = 'layouts' and split_part(path,'/',3) = 'releases' and split_part(path,'/',4) != '' and split_part(path,'/',5) = ''  then 'layouts_release_detail'
     WHEN split_part(path,'/',2) = 'layouts' and split_part(path,'/',3) = 'releases' and split_part(path,'/',4) != '' and split_part(path,'/',5) = 'edit'  then 'layouts_release_editor'
     WHEN split_part(path,'/',2) = 'layouts' and split_part(path,'/',3) != '' and split_part(path,'/',4) = 'variations' and rtrim(split_part(title,'|',0)) = 'HyperScience' then 'view_layout_variation'
     WHEN split_part(path,'/',2) = 'layouts' and split_part(path,'/',3) != '' and split_part(path,'/',4) = 'variations' and rtrim(split_part(title,'|',0)) = 'View Layout Variation' then 'view_layout_variation'
     WHEN split_part(path,'/',2) = 'layouts' and split_part(path,'/',3) != '' and split_part(path,'/',4) = 'variations' and rtrim(split_part(title,'|',0)) = 'Edit Layout Variation' then 'edit_layout_variation'
     WHEN split_part(path,'/',2) = 'layouts' and split_part(path,'/',3) != '' and split_part(path,'/',4) = 'variations' and rtrim(split_part(title,'|',0)) = 'Library' then 'layouts_library'
     WHEN split_part(path,'/',2) = 'layouts' and split_part(path,'/',3) != '' and split_part(path,'/',4) = 'variations' and rtrim(split_part(title,'|',0)) = 'Release Details' then 'layouts_release_detail'
     WHEN split_part(path,'/',2) = 'api' then 'api'
     WHEN split_part(path,'/',2) = 'login' then 'login'
     ELSE NULL end as pageview,
first_value(time) over (partition by session_id order by time asc) as session_start_time,
last_value(time) over (partition by session_id order by time asc) as session_end_time  
from "HEAP_MAIN_PRODUCTION"."HEAP"."PAGEVIEWS"
where split_part(domain,'.',0) in ('benefitmall-prod','kovack-prod','promomash-prod','transflo-prod')
order by customer, session_id, time asc
)

select * from pageviews