
select 

id,
lead_id,
created_date

from {{ ref('stg_lead_history') }}
where new_value = 'MQL' and field = 'Lifecycle_Status__c' and is_deleted = false