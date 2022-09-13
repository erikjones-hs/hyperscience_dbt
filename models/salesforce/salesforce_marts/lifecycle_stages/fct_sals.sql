with 

-- all leads with a lifecycle status change to SAL
sals as (

select 

id,
lead_id,
old_value,
created_date

from {{ ref('stg_lead_history') }}
where new_value = 'SAL' and field = 'Lifecycle_Status__c' and is_deleted = false

),

--leads that came into salesforce as SALs and therefore do not have lifecycle status change tracked
leads as (

select 

lead_id,
sal_date as created_date

from {{ ref('dim_leads_with_owner') }}
where sal_date is not null
and lead_id not in (select lead_id from sals)
and sal_date is not null

)

select 

id,
lead_id,
old_value,
created_date

from sals
union all
select

null as id,
lead_id,
null as old_value,
created_date

from leads
order by created_date desc



