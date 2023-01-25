with leads as (

    select 

    'lead' as type,
    lead_type as person_type,
    lead_id as person_id,
    first_name,
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
    contact_type as person_type,
    contact_id as person_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.global_region,
    c.sales_region,

    l.state as state,

    c.persona,
    c.lead_source,
    c.secondary_lead_source,
    c.first_lead_source_detail,
    c.last_lead_source,
    c.last_secondary_lead_source,
    c.last_lead_source_detail,
    c.lead_score,

    l.lead_score_at_mql as lead_score_at_mql,

    c.profile_score,
    c.engagement_score,
    c.lifecycle_status as lifecycle_status,

    c.contact_status as status,
    l.type_of_mql as type_of_mql,
    l.dq_reason as dq_reason,
    l.dq_reason_description as dq_reason_description,
    c.company_name as company_name,
    c.annual_revenue as annual_revenue,
    c.number_of_employees as number_of_employees,

    c.country,
    c.job_function,
    c.job_level,
    c.job_title,

    c.industry as industry,

    c.created_date,
    c.mal_date,
    c.mel_date,
    c.mql_date,
    c.sal_date,
    c.sel_date,
    c.sql_date,
    c.mrl_date,
    c.srl_date,

    c.dq_date as dq_date,

    c.customer_date,
    c.former_customer_date,

    l.converted_date as converted_date,

    c.owner_full_name, 
    c.owner_role_name

    from {{ ref('dim_contacts_with_owner') }} c
    left join {{ ref('dim_leads_with_owner') }} l
    on contact_id = converted_contact_id
    where c.is_deleted = false

)

select *
from leads 
union all
select * 
from contacts