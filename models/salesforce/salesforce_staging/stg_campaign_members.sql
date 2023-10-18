
select 

    id,
    is_deleted,
    campaign_id,
    lead_id,
    contact_id,
    status,
    has_responded,
    created_date,
    created_by_id,
    first_responded_date,
    type,
    utm_source_c,
    utm_medium_c,
    utm_campaign_c,
    utm_term_c,
    utm_content_c,
    form_host_url_c,
    referrer_url_c

from {{ source('salesforce', 'campaign_member') }}
