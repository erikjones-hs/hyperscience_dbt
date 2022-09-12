with 

-- all leads with a lifecycle status change to MEL
mels as (

select 

id,
lead_id,
created_date

from {{ ref('stg_lead_history') }}
where new_value = 'MEL' and field = 'Lifecycle_Status__c' and is_deleted = false

),

--leads that came into salesforce as MELs and therefore do not have lifecycle status change tracked
leads as (

select 

lead_id,
created_date

from {{ ref('dim_leads_with_owner') }}
where lead_id not in (select lead_id from mels)
and (mel_date >= '2022-03-24' or lead_source = 'Marketing') --history field tracking activated for lifecycle status on this date

)

select 

id,
lead_id,
created_date

from mels
union all
select

null as id,
lead_id,
created_date

from leads
order by created_date desc