
select 

id,
lead_id,
old_value,
created_date

from {{ ref('stg_lead_history') }}
where new_value = 'Disqualified' and field = 'Lifecycle_Status__c' and is_deleted = false
order by 4 desc