with 

campaign as (

    select *
    from {{ ref('stg_campaigns') }}
    where is_deleted = false

),

campaign_member as (

    select *
    from {{ ref('stg_campaign_members') }}
    where is_deleted = false

)

select 

    campaign_member.id as member_id,
    campaign_member.campaign_id as campaign_id,
    campaign_member.lead_id,
    campaign_member.contact_id,
    campaign_member.status as member_status,
    campaign_member.has_responded,
    campaign_member.created_date as member_created_date,
    campaign_member.created_by_id as member_created_by_id,
    campaign_member.first_responded_date,
    campaign_member.utm_source_c,
    campaign_member.utm_medium_c,
    campaign_member.utm_campaign_c,
    campaign_member.utm_term_c,
    campaign_member.utm_content_c,
    campaign_member.form_host_url_c,
    campaign_member.referrer_url_c,

    campaign.campaign_name,
    campaign.parent_id as parent_campaign_id,
    campaign.type as campaign_type,
    campaign.status as campaign_status,
    campaign.start_date as campaign_start_date,
    campaign.end_date as campaign_end_date,
    campaign.is_active as is_campaign_active,
    campaign.description as campaign_description,
    campaign.owner_id as campaign_owner_id,
    campaign.created_date as campaign_created_date,
    campaign.created_by_id as campaign_created_by_id

from campaign_member 
left join campaign 
on campaign_member.campaign_id = campaign.id