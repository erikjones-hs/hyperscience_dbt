
with
 
touchpoints as (
 
    select 
        *
    from {{ ref('stg_bizible_touchpoints') }}
 
),
 
attribution_touchpoints as (
 
    select
        *
    from {{ ref('stg_bizible_attribution_touchpoints') }}
 
),
 
fields as (
 
    select 
 
    unique_id,
    touchpoint_id,
    touchpoint_date,
    touchpoint_created_date,
    touchpoint_type,
    touchpoint_position,
    touchpoint_source,
    touchpoint_medium,
    channel as touchpoint_channel,
    ad_campaign_id as campaign_id,
    -- standardise the format of the campaign names
    upper(replace(ad_campaign_name, ' - ', '-')) as campaign_name,
    form_url,
    landing_page,
    bizible_person_id,
    contact_id,
    lead_id,
    opportunity_id
 
    from attribution_touchpoints
 
    union all
    
    select 
 
    unique_id,
    touchpoint_id,
    touchpoint_date,
    touchpoint_created_date,
    touchpoint_type,
    touchpoint_position,
    touchpoint_source,
    touchpoint_medium,
    channel as touchpoint_channel,
    ad_campaign_id as campaign_id,
    -- standardise the format of the campaign names
    upper(replace(ad_campaign_name, ' - ', '-')) as campaign_name,
    form_url,
    landing_page,
    bizible_person_id,
    contact_id,
    lead_id,
    opportunity_id
 
    from touchpoints
 
),
 
final as (
 
    select 
 
    unique_id,
    touchpoint_id,
    touchpoint_date,
    touchpoint_created_date,
    touchpoint_type,
    touchpoint_position,
    touchpoint_source,
    touchpoint_medium,
    touchpoint_channel,
    campaign_id,
    replace(iff(
        regexp_count(left(campaign_name, 1), '^[0-9]+$') > 0,
        right(campaign_name, len(campaign_name) - charindex('-', campaign_name)),
        campaign_name
    ), '_', ' ') as campaign_name,
    form_url,
    landing_page,
    bizible_person_id,
    contact_id,
    lead_id,
    opportunity_id
 
    from fields
 
)
 
select *
from final

