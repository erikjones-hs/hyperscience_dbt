
select 
 
id as lead_id,
account_name_c as account_id,
converted_opportunity_id,
converted_contact_id,
owner_id,
created_by_id, 
first_name,
last_name,
email,
phone,
global_region_c as global_region,
sales_region_c as sales_region,
state,
persona_c as persona,
lead_source,
secondary_lead_source_c as secondary_lead_source,
source_first_lead_source_detail_c as first_lead_source_detail,
source_last_person_source_c as last_lead_source,
source_last_secondary_lead_source_c as last_secondary_lead_source,
source_last_lead_source_detail_c as last_lead_source_detail,
lead_score_c as lead_score,
score_at_mql_c as lead_score_at_mql,
profile_score_c as profile_score,
engagement_score_c as engagement_score,
status,
lifecycle_status_c as lifecycle_status,
type_of_mql_c as type_of_mql,
qualification_notes_c as qualification_notes,
disqualified_picklist_c as dq_reason,
disqualified_reason_description_c as dq_reason_description,
    
-- combining company name fields based on priority
ifnull(company, zoom_info_company_name_c) as company_name,
    
-- combining annual revenue fields based on priority
ifnull(annual_revenue, zoom_info_annual_revenue_c) as annual_revenue,
    
-- combining number of employees fields based on priority
ifnull(number_of_employees, zoom_info_no_employees_c) as number_of_employees,
    
-- combining three country fields based on priority
ifnull(country, ifnull(inferred_country_c, zoom_info_country_c)) as country,
    
-- combining job function fields based on priority
ifnull(job_function_cleaned_c, zoom_info_job_function_c) as job_function,
    
-- combining job level fields based on priority
ifnull(job_level_cleaned_c, zoom_info_management_level_c) as job_level,
    
-- combining job title fields based on priority
ifnull(title, zoom_info_job_title_c) as job_title,
    
--combining industry fields based on priority
ifnull(industry_cleaned_c, zisf_zoom_info_industry_c) as industry,
    
is_converted,
is_deleted,
 
-- date fields
    
date(created_date) as created_date,
date(date_stage_mal_c) as mal_date,
date(date_stage_mel_c) as mel_date,
    
-- combining a historical mql date and current mql date fields
date(ifnull(date_stage_mql_c, mql_check_date_c)) as mql_date,
    
-- combining a historical sal stage date and a current sal stage date
-- note: from a marketing point of view sal means an SDR accepted the lead, but from a SDR point of view this lead was moved into working
date(ifnull(date_stage_sal_c, working_date_c)) as sal_date,
    
-- note: from a marketing point of view, working means the SDR has engaged with the lead, i.e scheduled a call, but the field name is 'working' this does not relate to the lead status of working and instead related to call_scheduled 
date(date_stage_working_c) as sel_date,
    
-- combining historical sql date and current sql date fields
date(ifnull(date_stage_sql_c, sql_check_date_c)) as sql_date,
    
date(date_stage_mrl_c) as mrl_date,
date(date_stage_srl_c) as srl_date,
 
-- combining historical dq date with the current dq date fields
date(ifnull(date_stage_disqualifed_c, dq_date_c)) as dq_date,
    
date(date_stage_customer_c) as customer_date,
date(date_stage_former_customer_c) as former_customer_date,
date(converted_date) as converted_date
 
from {{ source('salesforce', 'lead')}}