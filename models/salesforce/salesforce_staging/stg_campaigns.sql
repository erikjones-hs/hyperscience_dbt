
select 

    id,
    is_deleted,
    name as campaign_name,
    parent_id,
    type,
    status,
    start_date,
    end_date,
    is_active,
    description,
    owner_id,
    created_date,
    created_by_id

from {{ source('salesforce', 'campaign') }}