
select 

id,
lead_id,
created_by_id,
created_date,
new_value

from {{ ref('stg_lead_history') }}
where old_value = 'Needs Action' and field = 'Status'