select 
 
id as contact_id,
account_id,
owner_id, 
created_by_id, 
first_name,
last_name,
email,
phone,
global_region_c as global_region,
sales_region_c as sales_region,
persona_c as persona,
lead_source,
secondary_lead_source_c as secondary_lead_source,
source_first_lead_source_detail_c as first_lead_source_detail,
source_last_person_source_c as last_lead_source,
source_last_secondary_lead_source_c as last_secondary_lead_source,
source_last_lead_source_detail_c as last_lead_source_detail,
lead_score_c as lead_score,
profile_score_c as profile_score,
engagement_score_c as engagement_score,
qualification_notes_c as qualification_notes,
    
-- combining two country fields based on priority
ifnull(inferred_country_c, zoom_info_country_c) as country,
    
zoom_info_job_function_c as job_function,
    
-- combining job level fields based on priority
zoom_info_management_level_c as job_level,
    
-- combining job title fields based on priority
ifnull(title, zoom_info_job_title_c) as job_title,
 
is_deleted,
 
-- date fields
    
date(created_date) as created_date,
date(date_stage_mal_c) as mal_date,
date(date_stage_mel_c) as mel_date,
date(date_stage_mql_c) as mql_date,
date(date_stage_sal_c) as sal_date,
    
-- note: from a marketing point of view, working means the SDR has engaged with the lead, i.e scheduled a call, but the field name is 'working' this does not relate to the lead status of working and instead related to call_scheduled 
date(date_stage_working_c) as sel_date,
    
-- combining historical sql date and current sql date fields
date(date_stage_sql_c) as sql_date,
    
date(date_stage_mrl_c) as mrl_date,
date(date_stage_srl_c) as srl_date,
date(date_stage_customer_c) as customer_date,
date(date_stage_former_customer_c) as former_customer_date
 
from {{ source('salesforce', 'contact')}}