with contact_accounts as (

  select 

  a.member_id,
  a.lead_id,
  a.contact_id,
  b.account_id,
  a.created_date,
  campaign_type,
  campaign_name

  from {{ ref('fct_campaign_engagement') }} a
  left join {{ ref('dim_contacts_with_owner') }} b
  using (contact_id)
  where contact_id is not null

),
  
lead_accounts AS (
  
  select
    
  a.member_id,
  a.lead_id,
  a.contact_id,
  b.account_id,
  a.created_date,
  campaign_type,
  campaign_name

  from {{ ref('fct_campaign_engagement') }} a
  left join {{ ref('dim_leads_with_owner') }} b
  using (lead_id)
  where contact_id is null

)

select *
from contact_accounts
union all
select *
from lead_accounts