with leads as (

    select 

    'lead' as type,
    first_name,
    lead_id as person_id,
    last_name,
    email,
    phone,
    global_region,
    sales_region,
    state,
    persona,
    lead_source,
    secondary_lead_source,
    first_lead_source_detail,
    last_lead_source,
    last_secondary_lead_source,
    last_lead_source_detail,
    lead_score,
    lead_score_at_mql,
    profile_score,
    engagement_score,
    lifecycle_status,
    status,
    lead_type,
    type_of_mql,
    dq_reason,
    dq_reason_description,
    company_name,
    annual_revenue,
    number_of_employees,
    country,
    job_function,
    job_level,
    job_title,
    industry,
    created_date,
    mal_date,
    mel_date,
    mql_date,
    sal_date,
    sel_date,
    sql_date,
    mrl_date,
    srl_date,
    dq_date,
    customer_date,
    former_customer_date,
    converted_date,
    owner_full_name, 
    owner_role_name

    from {{ ref('dim_leads_with_owner') }}
    where is_deleted = false and is_converted = false

),

contacts as (

    select

    'contact' as type,
    contact_id as person_id,
    first_name,
    last_name,
    email,
    phone,
    global_region,
    sales_region,
    null as state,
    persona,
    lead_source,
    secondary_lead_source,
    first_lead_source_detail,
    last_lead_source,
    last_secondary_lead_source,
    last_lead_source_detail,
    lead_score,
    null as lead_score_at_mql,
    profile_score,
    engagement_score,
    null as lifecycle_status,
    null as status,
    null as lead_type,
    null as type_of_mql,
    null as dq_reason,
    null as dq_reason_description,
    null as company_name,
    null as annual_revenue,
    null as number_of_employees,
    country,
    job_function,
    job_level,
    job_title,
    null as industry,
    created_date,
    mal_date,
    mel_date,
    mql_date,
    sal_date,
    sel_date,
    sql_date,
    mrl_date,
    srl_date,
    null as dq_date,
    customer_date,
    former_customer_date,
    null as converted_date,
    owner_full_name, 
    owner_role_name

    from {{ ref('dim_contacts_with_owner') }}
    where is_deleted = false

)

select *
from leads 
union all
select * 
from contacts