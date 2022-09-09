


select 

id,
lead_id,
created_date

from {{ ref('stg_lead_history') }}
where field = 'created' and is_deleted = false
