

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
    campaign_name,
    form_url,
    landing_page,
    bizible_person_id,
    contact_id,
    lead_id,
    opportunity_id

from {{ ref('stg_bizible_touchpoints_final') }}