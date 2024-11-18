{{ config(materialized='table')}}

select
    id
    ,attribution_contact_email
    ,attribution_contact_id
    ,attribution_hubspot_contact_id
    ,acquisition_channel
    ,acquisition_channel_detail
    ,acquisition_channel_campaign
    ,acquisition_channel_keyword
    ,lead_creation_source
    ,lead_creation_source_detail
from {{ ref('int_sf_opportunity')}}
where
    attribution_contact_id is not null