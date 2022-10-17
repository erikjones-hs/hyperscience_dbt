
select 

id,
created_date,
opportunity_id,
contact_id,
role,
is_primary,
created_by_id

from {{ source('salesforce', 'opportunity_contact_role') }}
where is_deleted = false