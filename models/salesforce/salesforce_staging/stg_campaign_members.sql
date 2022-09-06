
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
    first_responded_date

from {{ source('salesforce', 'campaign_member') }}
