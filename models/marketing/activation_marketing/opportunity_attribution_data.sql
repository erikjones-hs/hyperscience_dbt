{{ config(materialized='table')}}

select
    id
    ,hubspot_deal_id
    ,attribution_contact_email
    ,attribution_contact_id
    ,attribution_hubspot_contact_id
    ,acquisition_channel
    ,acquisition_channel_detail
    ,acquisition_channel_campaign
    ,acquisition_channel_keyword
    ,lead_creation_source
    ,lead_creation_source_detail
    ,latest_channel
    ,latest_channel_detail
    ,latest_channel_campaign
    ,latest_channel_keyword
    ,latest_lead_source
    ,latest_lead_source_detail
from {{ ref('int_sf_opportunity')}}
where
    attribution_contact_id is not null