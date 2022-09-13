
with 

-- all leads with a lifecycle status change to MQL
mqls as (

select 

id,
lead_id,
old_value,
created_date

from {{ ref('stg_lead_history') }}
where new_value = 'MQL' and field = 'Lifecycle_Status__c' and is_deleted = false

),

--leads that came into salesforce as MQLs and therefore do not have lifecycle status change tracked
leads as (

select 

lead_id,
mql_date as created_date

from {{ ref('dim_leads_with_owner') }}
where mql_date is not null
and lead_id not in (select lead_id from mqls)
and mql_date is not null --history field tracking activated for lifecycle status on this date

)

select 

id,
lead_id,
old_value,
created_date

from mqls
union all
select

null as id,
lead_id,
null as old_value,
created_date

from leads
order by created_date desc



