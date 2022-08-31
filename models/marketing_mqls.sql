{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with mqls as (
select
to_timestamp(lclcss.date) as mql_dte,
lclcss.person_id,
slac.lead_source,
slac.email,
slac.lead_type,
slac.secondary_lead_source,
slac.source_first_lead_source_detail,
slac.last_lead_source,
CASE WHEN slac.email IS NOT NULL THEN split_part(slac.email,'@',2) ELSE NULL end as email_domain
from {{ref('lead_contact_life_cycle_status_changes')}} as lclcss
left join {{ref('salesforce_leads_and_contacts')}} as slac 
    on (lclcss.person_id = slac.person_id)
where lclcss.status_change in ('MQL from SAL','MQL')
and slac.lead_type = 'Sales'
and (email_domain IS NULL OR email_domain not in ('hyperscience.com'))
)

select * from mqls