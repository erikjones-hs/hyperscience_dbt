
select 

    id,
    lead_id,
    created_by_id,
    created_date,
    field,
    data_type,
    old_value,
    new_value,
    is_deleted

from {{ source('salesforce', 'lead_history') }}