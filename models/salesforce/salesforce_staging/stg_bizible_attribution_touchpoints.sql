
with 
 
touchpoints as (
    
    select 
    id as touchpoint_id,
    bizible_2_unique_id_c as unique_id,
    bizible_2_touchpoint_date_c as touchpoint_date,
    bizible_2_touchpoint_position_c as touchpoint_position,
    bizible_2_touchpoint_source_c as touchpoint_source,
    bizible_2_touchpoint_type_c as touchpoint_type,
    created_date as touchpoint_created_date,
    cast(bizible_2_ad_campaign_id_c as string) as ad_campaign_id,
    bizible_2_ad_campaign_name_c as ad_campaign_name,
    bizible_2_ad_content_c as ad_content,
    bizible_2_ad_destination_url_c as ad_destination_url,
    bizible_2_ad_group_id_c as ad_group_id,
    bizible_2_ad_group_name_c as ad_group_name,
    bizible_2_ad_id_c ad_id,
    bizible_2_count_first_touch_c as first_touch_count,
    bizible_2_count_lead_creation_touch_c as lead_creation_touch_count,
    bizible_2_count_u_shaped_c u_shaped_count,
    bizible_2_form_url_c as form_url,
    bizible_2_form_url_raw_c as form_url_raw,
    bizible_2_keyword_id_c as keyword_id,
    bizible_2_keyword_match_type_c as match_type,
    bizible_2_keyword_text_c as keyword_text,
    bizible_2_landing_page_c as landing_page,
    bizible_2_landing_page_raw_c as landing_page_raw,
    bizible_2_marketing_channel_c as channel,
    bizible_2_marketing_channel_path_c as channel_path,
    bizible_2_medium_c as touchpoint_medium,
    bizible_2_placement_id_c as placement_id,
    bizible_2_placement_name_c as placement_name,
    bizible_2_platform_c as platform,
    bizible_2_referrer_page_c as referrer_page,
    bizible_2_referrer_page_raw_c as referrer_page_raw,
    bizible_2_search_phrase_c as search_phrase,
    bizible_2_segment_c as segment,
    bizible_2_sf_campaign_c as sf_campaign_name,
    bizible_2_site_id_c as site_id,
    bizible_2_site_name_c as site_name,
 
    -- the opportunity id that this touchpoint has influenced
    bizible_2_opportunity_c as opportunity_id,
 
    -- the contact id that this touchpoint was associated too
    bizible_2_contact_c as contact_id
 
    from {{ source('salesforce', 'bizible_2_bizible_attribution_touchpoint_c') }}
    where is_deleted = false
 
)
 
select *
from touchpoints
left join {{ ref('stg_bizible_person') }} as bizible_person
using (contact_id)




