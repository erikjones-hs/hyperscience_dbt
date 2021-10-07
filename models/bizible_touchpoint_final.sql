{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MARKETING'
)
}}


WITH user AS (
  
  SELECT
  
  id as user_id,
  name as full_name
  
  FROM "FIVETRAN_DATABASE"."SALESFORCE"."USER"
  GROUP BY id, name

),

account AS (

  SELECT
  
  id as account_id,
  name as account_name,
  industry_cleaned_c as account_industry,
  global_account_id_c as global_account_id,
  created_date as account_created_date,
  sales_region_c as account_sales_region,
  type as account_type,
  owner_id as account_owner_id,
  user.full_name as account_owner_name,
  abm_target_type_c as abm_target_type,
  account_manager_c as account_manager,
  annual_revenue as annual_revenue,
  billing_country as billing_country,
  billing_postal_code as billing_postal_code,
  billing_state as billing_state,
  bizible_2_engagement_score_c as bizible_account_score,
  company_size_c as company_size,
  number_of_employees as account_number_of_employees,
  csm_c as account_csm,
  domain_c as account_domain
  
  FROM "FIVETRAN_DATABASE"."SALESFORCE"."ACCOUNT" as account
  LEFT JOIN user
    ON account.owner_id = user_id
  WHERE is_deleted = false
  
  ),
  
contact AS (
  
  SELECT
  
  id as contact_id,
  account_id as account_id,
  authority_c as contact_authority,
  budget_c  as contact_budget,
  contact_type_c as contact_type,
  created_date as contact_created_date,
  email as contact_email,
  global_region_c as contact_global_region,
  lead_source as contact_lead_source,
  sales_region_c as contact_sales_region,
  title as contact_title,
  mql_c as contact_mql_checkbox
  
  FROM "FIVETRAN_DATABASE"."SALESFORCE"."CONTACT"
  WHERE is_deleted = false
  
),


lead AS (
  
  SELECT
  
  abm_target_type_c as lead_abm_target_type,
  annual_revenue as lead_annual_revenue,
  authority_c as authority,
  budget_c as budget,
  company as lead_company,
  converted_date as lead_converted_date,
  country as lead_country,
  created_date as lead_created_date,
  email as lead_email,
  id as lead_id,
  is_converted as lead_is_converted,
  lead_score_c as lead_score,
  lead_source as lead_source,
  number_of_employees as lead_number_of_employees,
  owner_id as lead_owner_id,
  sales_region_c as lead_sales_region,
  state as lead_state,
  title as lead_title,
  website as lead_website,
  mql_c as lead_mql_checkbox,
  working_stage_checkbox_c as lead_working_stage_checkbox,
  status as lead_status,
  dq_date_c as lead_dq_date,
  disqualified_picklist_c as lead_disqualified_reason,
  disqualified_reason_description_c as lead_disqualified_reason_description
  
  FROM "FIVETRAN_DATABASE"."SALESFORCE"."LEAD"
  WHERE is_deleted = false

),

opportunity AS (
  
  SELECT
  
  amount as opportunity_amount,
  forecasted_arr_c as forecasted_arr,
  arr_mirror_c as mirror_arr,
  close_date as opportunity_close_date,
  created_by_id opportunity_created_by_id,
  created_date as opportunity_created_date,
  id as opportunity_id,
  is_closed as opportunity_is_closed,
  is_won as opportunity_is_won,
  last_activity_date as opportunity_last_activity_date,
  name as opportunity_name,
  owner_id as opportunity_owner_id,
  user.full_name as opportunity_owner_name,
  probability as opportunity_probability,
  stage_name as opportunity_stage,
  start_date_c as opportunity_start_date,
  vo_check_c as opportunity_vo_check,
  vo_date_c as opportunity_vo_date,
  end_renewal_date_c as opportunity_end_renewal_date,
  closed_won_date_c as opportunity_closed_won_date,
  closed_lost_date_c as opportunity_closed_lost_date,
  revenue_type_c as revenue_type
  
  FROM "FIVETRAN_DATABASE"."SALESFORCE"."OPPORTUNITY"as opportunity
   LEFT JOIN user
    ON opportunity.owner_id = user_id
  WHERE is_deleted = false

),

bizible_person AS (
  
SELECT

  RIGHT(id,18) as bizible_person_id,
  bizible_2_contact_c as contact_id,
  bizible_2_lead_c as lead_id,
  created_date as bizible_person_created_date,
  name as email,
  COUNT(DISTINCT bizible_2_unique_id_c) as bizible_person_count
  
  FROM "FIVETRAN_DATABASE"."SALESFORCE"."BIZIBLE_2_BIZIBLE_PERSON_C"
  WHERE is_deleted = false
  GROUP BY id, bizible_2_unique_id_c, bizible_2_contact_c, bizible_2_lead_c, created_date, name

),

touchpoint AS (
  
  SELECT
  
  bizible_2_account_c as account_id,
  bizible_2_ad_campaign_id_c as ad_campaign_id,
  bizible_2_ad_campaign_name_c as ad_campaign_name,
  bizible_2_ad_content_c as ad_content,
  bizible_2_ad_destination_url_c as ad_destination_url,
  bizible_2_ad_group_id_c as ad_group_id,
  bizible_2_ad_group_name_c as ad_group_name,
  bizible_2_ad_id_c ad_id,
  0 as custom_model_attribution_2,
  0 as custom_model_attribution,
  0 as first_touch_attribution,
  0 as lead_conversion_touch_attribution,
  0 as u_shaped_attribution,
  0 as w_shaped_attribution,
  bizible_2_bizible_person_c as bizible_person_id,
  bizible_2_contact_c as contact_id,
  0 as custom_model_count_2,
  0 as custom_model_count,
  bizible_2_count_first_touch_c as first_touch_count,
  bizible_2_count_lead_creation_touch_c as lead_creation_touch_count,
  bizible_2_count_u_shaped_c u_shaped_count,
  0 as w_shaped_count,
  bizible_2_form_url_c as form_url,
  bizible_2_form_url_raw_c as form_url_raw,
  bizible_2_keyword_id_c as keyword_id,
  bizible_2_keyword_match_type_c as match_type,
  bizible_2_keyword_text_c as keyword_text,
  bizible_2_landing_page_c as landing_page,
  bizible_2_landing_page_raw_c as landing_page_raw,
  bizible_2_marketing_channel_c as channel,
  bizible_2_marketing_channel_path_c as channel_path,
  bizible_2_medium_c as medium,
  '' as opportunity_id,
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
  bizible_2_touchpoint_date_c as touchpoint_date,
  bizible_2_touchpoint_position_c as touchpoint_position,
  bizible_2_touchpoint_source_c as touchpoint_source,
  bizible_2_touchpoint_type_c as touchpoint_type,
  bizible_2_unique_id_c as unique_id,
  created_date as touchpoint_created_date,
  id as touchpoint_id,
  'BT' as object
  
  FROM "FIVETRAN_DATABASE"."SALESFORCE"."BIZIBLE_2_BIZIBLE_TOUCHPOINT_C"
  WHERE is_deleted = false

),

attribution_touchpoint AS (
  
  SELECT
  
  bat.bizible_2_account_c as account_id,
  bat.bizible_2_ad_campaign_id_c as ad_campaign_id,
  bat.bizible_2_ad_campaign_name_c as ad_campaign_name,
  bat.bizible_2_ad_content_c as ad_content,
  bat.bizible_2_ad_destination_url_c as ad_destination_url,
  bat.bizible_2_ad_group_id_c as ad_group_id,
  bat.bizible_2_ad_group_name_c as ad_group_name,
  bat.bizible_2_ad_id_c ad_id,
  bat.bizible_2_attribution_custom_model_2_c as custom_model_attribution_2,
  bat.bizible_2_attribution_custom_model_c as custom_model_attribution,
  bat.bizible_2_attribution_first_touch_c as first_touch_attribution,
  bat.bizible_2_attribution_lead_conversion_touch_c as lead_conversion_touch_attribution,
  bat.bizible_2_attribution_u_shaped_c as u_shaped_attribution,
  bat.bizible_2_attribution_w_shaped_c as w_shaped_attribution,

  bp.id as bizible_person_id,
  
  bat.bizible_2_contact_c as contact_id,
  bat.bizible_2_count_custom_model_2_c as custom_model_count_2,
  bat.bizible_2_count_custom_model_c as custom_model_count,
  bat.bizible_2_count_first_touch_c as first_touch_count,
  bat.bizible_2_count_lead_creation_touch_c as lead_creation_touch_count,
  bat.bizible_2_count_u_shaped_c as u_shaped_count,
  bat.bizible_2_count_w_shaped_c as w_shaped_count,
  bat.bizible_2_form_url_c as form_url,
  bat.bizible_2_form_url_raw_c as form_url_raw,
  bat.bizible_2_keyword_id_c as keyword_id,
  bat.bizible_2_keyword_match_type_c as match_type,
  bat.bizible_2_keyword_text_c as keyword_text,
  bat.bizible_2_landing_page_c as landing_page,
  bat.bizible_2_landing_page_raw_c as landing_page_raw,
  bat.bizible_2_marketing_channel_c as channel,
  bat.bizible_2_marketing_channel_path_c as channel_path,
  bat.bizible_2_medium_c as medium,
  bat.bizible_2_opportunity_c as opportunity_id,
  bat.bizible_2_placement_id_c as placement_id,
  bat.bizible_2_placement_name_c as placement_name,
  bat.bizible_2_platform_c as platform,
  bat.bizible_2_referrer_page_c as referrer_page,
  bat.bizible_2_referrer_page_raw_c as referrer_page_raw,
  bat.bizible_2_search_phrase_c as search_phrase,
  bat.bizible_2_segment_c as segment,
  bat.bizible_2_sf_campaign_c as sf_campaign_name,
  bat.bizible_2_site_id_c as site_id,
  bat.bizible_2_site_name_c as site_name,
  bat.bizible_2_touchpoint_date_c as touchpoint_date,
  bat.bizible_2_touchpoint_position_c as touchpoint_position,
  bat.bizible_2_touchpoint_source_c as touchpoint_source,
  bat.bizible_2_touchpoint_type_c as touchpoint_type,
  bat.bizible_2_unique_id_c as unique_id,
  bat.created_date as touchpoint_created_date,
  bat.id as touchpoint_id,
  'BAT' as object
  
  FROM "FIVETRAN_DATABASE"."SALESFORCE"."BIZIBLE_2_BIZIBLE_ATTRIBUTION_TOUCHPOINT_C" as bat
  LEFT JOIN "FIVETRAN_DATABASE"."SALESFORCE"."BIZIBLE_2_BIZIBLE_PERSON_C" as bp
  ON bat.bizible_2_contact_c = bp.bizible_2_contact_c
  WHERE bat.is_deleted = false

),

touchpoint_combined AS (
  
  SELECT
      
  (touchpoint.account_id),
  (touchpoint.ad_campaign_id),
  (touchpoint.ad_campaign_name),
  (touchpoint.ad_content),
  (touchpoint.ad_destination_url),
  (touchpoint.ad_group_id),
  (touchpoint.ad_group_name),
  (touchpoint.ad_id),
  (touchpoint.custom_model_attribution),
  (touchpoint.custom_model_attribution_2),
  (touchpoint.custom_model_count),
  (touchpoint.custom_model_count_2),
  (touchpoint.first_touch_attribution),
  (touchpoint.first_touch_count),
  (touchpoint.form_url),
  (touchpoint.form_url_raw),
  (touchpoint.keyword_id),
  (touchpoint.keyword_text),
  (touchpoint.landing_page),
  (touchpoint.landing_page_raw),
  (touchpoint.lead_conversion_touch_attribution),
  (touchpoint.lead_creation_touch_count),
  (touchpoint.channel),
  (touchpoint.channel_path),
  (touchpoint.match_type),
  (touchpoint.medium),
  (touchpoint.object),
  (touchpoint.opportunity_id),
  (touchpoint.placement_id),
  (touchpoint.placement_name),
  (touchpoint.platform),
  (touchpoint.referrer_page),
  (touchpoint.referrer_page_raw),
  (touchpoint.search_phrase),
  (touchpoint.segment),
  (touchpoint.sf_campaign_name),
  (touchpoint.site_id),
  (touchpoint.site_name),
  (DATE(touchpoint.touchpoint_created_date )) as touchpoint_created_date,
  (DATE(touchpoint.touchpoint_date)) as touchpoint_date,
  (touchpoint.touchpoint_id),
  (touchpoint.touchpoint_position),
  (touchpoint.touchpoint_source),
  (touchpoint.touchpoint_type),
  (touchpoint.u_shaped_attribution),
  (touchpoint.u_shaped_count),
  (touchpoint.unique_id),
  (touchpoint.w_shaped_attribution),
  (touchpoint.w_shaped_count),
  (DATE(bizible_person.bizible_person_created_date)) as bizible_person_created_date,
  (bizible_person.bizible_person_id) as bizible_person_id,
  (bizible_person.contact_id),
  (bizible_person.email),
  (bizible_person.lead_id)
  
  FROM touchpoint
  LEFT JOIN bizible_person
  ON (touchpoint.bizible_person_id) = (bizible_person.bizible_person_id)
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54

  UNION ALL

  SELECT
 
  (attribution_touchpoint.account_id),
  (attribution_touchpoint.ad_campaign_id),
  (attribution_touchpoint.ad_campaign_name),
  (attribution_touchpoint.ad_content),
  (attribution_touchpoint.ad_destination_url),
  (attribution_touchpoint.ad_group_id),
  (attribution_touchpoint.ad_group_name),
  (attribution_touchpoint.ad_id),
  (attribution_touchpoint.custom_model_attribution),
  (attribution_touchpoint.custom_model_attribution_2),
  (attribution_touchpoint.custom_model_count),
  (attribution_touchpoint.custom_model_count_2),
  (attribution_touchpoint.first_touch_attribution),
  (attribution_touchpoint.first_touch_count),
  (attribution_touchpoint.form_url),
  (attribution_touchpoint.form_url_raw),
  (attribution_touchpoint.keyword_id),
  (attribution_touchpoint.keyword_text),
  (attribution_touchpoint.landing_page),
  (attribution_touchpoint.landing_page_raw),
  (attribution_touchpoint.lead_conversion_touch_attribution),
  (attribution_touchpoint.lead_creation_touch_count),
  (attribution_touchpoint.channel),
  (attribution_touchpoint.channel_path),
  (attribution_touchpoint.match_type),
  (attribution_touchpoint.medium),
  (attribution_touchpoint.object),
  (attribution_touchpoint.opportunity_id),
  (attribution_touchpoint.placement_id),
  (attribution_touchpoint.placement_name),
  (attribution_touchpoint.platform),
  (attribution_touchpoint.referrer_page),
  (attribution_touchpoint.referrer_page_raw),
  (attribution_touchpoint.search_phrase),
  (attribution_touchpoint.segment),
  (attribution_touchpoint.sf_campaign_name),
  (attribution_touchpoint.site_id),
  (attribution_touchpoint.site_name),
  (DATE(attribution_touchpoint.touchpoint_created_date)) as touchpoint_created_date,
  (DATE(attribution_touchpoint.touchpoint_date)) as touchpoint_date,
  (attribution_touchpoint.touchpoint_id),
  (attribution_touchpoint.touchpoint_position),
  (attribution_touchpoint.touchpoint_source),
  (attribution_touchpoint.touchpoint_type),
  (attribution_touchpoint.u_shaped_attribution),
  (attribution_touchpoint.u_shaped_count),
  (attribution_touchpoint.unique_id),
  (attribution_touchpoint.w_shaped_attribution),
  (attribution_touchpoint.w_shaped_count),
  (DATE(bizible_person.bizible_person_created_date)) as bizible_person_created_date,
  (bizible_person.bizible_person_id) as bizible_person_id,
  (bizible_person.contact_id),
  (bizible_person.email),
  (bizible_person.lead_id)
  
  FROM attribution_touchpoint
  LEFT JOIN bizible_person
  ON (attribution_touchpoint.bizible_person_id) = (bizible_person.bizible_person_id)
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54

),

touchpoint_final AS (

  SELECT
  
  (touchpoint_combined.ad_campaign_id),
  (touchpoint_combined.ad_campaign_name),
  (touchpoint_combined.ad_content),
  (touchpoint_combined.ad_destination_url),
  (touchpoint_combined.ad_group_id),
  (touchpoint_combined.ad_group_name),
  (touchpoint_combined.ad_id),
  (touchpoint_combined.custom_model_attribution),
  (touchpoint_combined.custom_model_attribution_2),
  (touchpoint_combined.custom_model_count),
  (touchpoint_combined.custom_model_count_2),
  (touchpoint_combined.first_touch_attribution),
  (touchpoint_combined.first_touch_count),
  (touchpoint_combined.form_url),
  (touchpoint_combined.form_url_raw),
  (touchpoint_combined.keyword_id),
  (touchpoint_combined.keyword_text),
  (touchpoint_combined.landing_page),
  (touchpoint_combined.landing_page_raw),
  (touchpoint_combined.lead_conversion_touch_attribution),
  (touchpoint_combined.lead_creation_touch_count),
  (touchpoint_combined.channel),
  (touchpoint_combined.channel_path),
  (touchpoint_combined.match_type),
  (touchpoint_combined.medium),
  (touchpoint_combined.object),
  (touchpoint_combined.placement_id),
  (touchpoint_combined.placement_name),
  (touchpoint_combined.platform),
  (touchpoint_combined.referrer_page),
  (touchpoint_combined.referrer_page_raw),
  (touchpoint_combined.search_phrase),
  (touchpoint_combined.segment),
  (touchpoint_combined.sf_campaign_name),
  (touchpoint_combined.site_id),
  (touchpoint_combined.site_name),
  (DATE(touchpoint_combined.touchpoint_created_date)) as touchpoint_created_date,
  (DATE(touchpoint_combined.touchpoint_date)) as touchpoint_date,
  (touchpoint_combined.touchpoint_id),
  (touchpoint_combined.touchpoint_position),
  (touchpoint_combined.touchpoint_source),
  (touchpoint_combined.touchpoint_type),
  (touchpoint_combined.u_shaped_attribution),
  (touchpoint_combined.u_shaped_count),
  (touchpoint_combined.unique_id),
  (touchpoint_combined.w_shaped_attribution),
  (touchpoint_combined.w_shaped_count),
  (DATE(touchpoint_combined.bizible_person_created_date)) as bizible_person_created_date,
  (touchpoint_combined.bizible_person_id),
  (touchpoint_combined.email),
   
  (opportunity.opportunity_amount),
  (opportunity.forecasted_arr) as opportunity_forecasted_arr,
  (opportunity.mirror_arr) as opportunity_mirror_arr,
  (DATE(opportunity.opportunity_close_date )) as opportunity_close_date,
  (opportunity.opportunity_created_by_id),
  (DATE(opportunity.opportunity_created_date )) as opportunity_created_date,
  (DATE(opportunity.opportunity_end_renewal_date )) as opportunity_end_renewal_date,
  (opportunity.opportunity_id),
  (opportunity.opportunity_is_closed),
  (opportunity.opportunity_is_won),
  (DATE(opportunity.opportunity_last_activity_date )) as opportunity_last_activity_date,
  (opportunity.opportunity_name),
  (opportunity.opportunity_owner_id),
  (opportunity.opportunity_probability),
  (opportunity.opportunity_stage),
  (DATE(opportunity.opportunity_start_date )) as opportunity_start_date,
  (opportunity.opportunity_vo_check),
  (DATE(opportunity.opportunity_vo_date )) as opportunity_vo_date,
  (DATE(opportunity.opportunity_closed_lost_date )) as opportunity_closed_lost_date,
  (DATE(opportunity.opportunity_closed_won_date )) as opportunity_closed_won_date, 
  (revenue_type) as opportunity_revenue_type,  
  (account.abm_target_type),
  (DATE(account.account_created_date )) as account_created_date,
  (account.account_csm),
  (account.account_domain),
  (account.account_id),
  (account.account_industry),
  (account.account_manager),
  (account.account_name),
  (account.account_number_of_employees),
  (account.account_owner_id),
  (account.account_sales_region),
  (account.account_type),
  (account.annual_revenue),
  (account.billing_country) ,
  (account.billing_postal_code),
  (account.billing_state),
  (account.bizible_account_score),
  (account.company_size),
  (account.global_account_id),
  (contact.contact_authority),
  (contact.contact_budget),
  (DATE(contact.contact_created_date )) as contact_created_date,
  (contact.contact_email),
  (contact.contact_global_region),
  (contact.contact_id),
  (contact.contact_lead_source),
  (contact.contact_sales_region),
  (contact.contact_title),
  (contact.contact_type),
  (contact.contact_mql_checkbox),
  (lead.authority),
  (lead.budget),
  (lead.lead_abm_target_type),
  (lead.lead_annual_revenue),
  (lead.lead_company),
  (DATE(lead.lead_converted_date )) as lead_converted_date,
  (lead.lead_country),
  (DATE(lead.lead_created_date )) as lead_created_date,
  (lead.lead_email),
  (lead.lead_id),
  (lead.lead_is_converted),
  (lead.lead_number_of_employees),
  (lead.lead_owner_id),
  (lead.lead_sales_region),
  (lead.lead_score),
  (lead.lead_source),
  (lead.lead_state),
  (lead.lead_title),
  (lead.lead_website),
  (lead.lead_mql_checkbox),
  (lead.lead_working_stage_checkbox),
  (lead.lead_status),
  (lead.lead_dq_date),
  (lead.lead_disqualified_reason),
  (lead.lead_disqualified_reason_description),
    (IFF(contact.contact_mql_checkbox = 'true', contact.contact_mql_checkbox, lead.lead_mql_checkbox)) as mql_checkbox,
    IFF(account.account_id IN (
        
        SELECT 
        
            ACCOUNT_ID as ACCOUNTS
    
        FROM FIVETRAN_DATABASE.SALESFORCE.OPPORTUNITY
        WHERE _FIVETRAN_ACTIVE = TRUE
        AND ACTIVE_OPPORTUNITY_C = TRUE
        AND FORECASTED_ARR_C >= 1
        AND ID NOT IN ('O0063600000iRvnHAAS','0063600000iRvm9AAC','0061R00000oFYN2QAO')
        ),
   TRUE, 
   FALSE) as active_customer,
  
  opportunity.opportunity_owner_name,
  account.account_owner_name



  FROM touchpoint_combined
  
  LEFT JOIN opportunity
    ON (touchpoint_combined.opportunity_id) = (opportunity.opportunity_id)

  LEFT JOIN account
    ON (touchpoint_combined.account_id) = (account.account_id)

  LEFT JOIN contact
    ON (touchpoint_combined.contact_id) = (contact.contact_id)

  LEFT JOIN lead
    ON (touchpoint_combined.lead_id) = (lead.lead_id)
  
  )
  
  SELECT 
  
    *,
    
    DATE(CASE
    WHEN touchpoint_position LIKE '%MQL%'
    AND mql_checkbox = 'true'
        THEN touchpoint_date 
    ELSE NULL
    END) as bizible_mql_date,
    
   DATE(CASE
    WHEN touchpoint_position LIKE '%SAL%'
    AND lead_working_stage_checkbox = 'true'    
        THEN touchpoint_date
    ELSE NULL
    END) as bizible_sal_date
  
  FROM touchpoint_final

