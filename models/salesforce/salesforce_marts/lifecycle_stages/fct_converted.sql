

select 

id,
lead_id,
old_value,
created_date

from {{ ref('stg_lead_history') }}
where new_value = 'Converted' and field = 'Status' and is_deleted = false

