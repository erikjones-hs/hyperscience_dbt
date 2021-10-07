{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MARKETING'
)
}}


WITH source AS (
  
  SELECT 
  
    --Overview
    
    ID as lead_id,
    FIRST_NAME as first_name,
    LAST_NAME as last_name,
    NAME as full_name,
    COUNTRY as country,
    
 
    --Lead Source Information
 
    LEAD_SOURCE as lead_source,
    SECONDARY_LEAD_SOURCE_C as secondary_lead_source,
    LEAD_SOURCE_ORIGINAL_C as original_lead_source,
    LEAD_SOURCE_MOST_RECENT_C as most_recent_lead_source,
    LEAD_SOURCE_CAMPAIGN_ORIGINAL_C as original_lead_source_campaign,
    LEAD_SOURCE_CAMPAIGN_MOST_RECENT_C as most_recent_lead_source_campaign,
    LEAD_SOURCE_OFFER_TYPE_ORIGINAL_C as original_lead_source_offer_type,
    LEAD_SOURCE_OFFER_TYPE_LAST_C as most_recent_lead_source_offer_type,
    LEAD_SOURCE_OFFER_ORIGINAL_C as original_lead_source_offer,
    LEAD_SOURCE_OFFER_LAST_C as most_recent_lead_source_offer,
    LEAD_SOURCE_AD_GROUP_ORIGINAL_C as original_lead_source_ad_group,
    
    BIZIBLE_2_TOUCHPOINT_DATE_FT_C as bizible_touchpoint_date_first_touch,
    BIZIBLE_2_MARKETING_CHANNEL_FT_C as bizible_touchpoint_marketing_channel_first_touch,
    BIZIBLE_2_AD_CAMPAIGN_NAME_FT_C as bizible_touchpoint_campaign_name_first_touch,
    BIZIBLE_2_TOUCHPOINT_SOURCE_FT_C as bizible_touchpoint_source_first_touch,
    BIZIBLE_2_LANDING_PAGE_FT_C as bizible_touchpoint_landing_page_first_touch,
    
    BIZIBLE_2_TOUCHPOINT_DATE_LC_C as bizible_touchpoint_date_lead_creation,
    BIZIBLE_2_MARKETING_CHANNEL_LC_C as bizible_touchpoint_marketing_channel_lead_creation,
    BIZIBLE_2_AD_CAMPAIGN_NAME_LC_C as bizible_touchpoint_campaign_name_lead_creation,
    BIZIBLE_2_TOUCHPOINT_SOURCE_LC_C as bizible_touchpoint_source_lead_creation,
    BIZIBLE_2_LANDING_PAGE_LC_C as bizible_touchpoint_landing_page_lead_creation,
    
    
 
    --Marketing Funnel 
    
    
    CREATED_DATE as lead_created_date,
    DATE_STAGE_MAL_C as mal_stage_date,
    DATE_STAGE_MEL_C as mel_stage_date,
    DATE_STAGE_MQL_C as mql_stage_date,
    DATE_STAGE_SAL_C as sal_stage_date,
    DATE_STAGE_SQL_C as sql_stage_date,
    DATE_STAGE_MRL_C as mrl_stage_date,
    DATE_STAGE_SRL_C as srl_stage_date,
    
    
    DATE_STAGE_CUSTOMER_C as customer_stage_date,
 
    DATE_STAGE_FORMER_CUSTOMER_C as former_customer_stage_date,
 
    DATE_STAGE_DISQUALIFED_C as disqualified_stage_date,
 
    DISQUALIFIED_PICKLIST_C as disqualified_reason,
 
    DISQUALIFIED_REASON_DESCRIPTION_C as disqualified_reason_description,
 
    MQL_C as mql_check,
    SQL_C as sql_check,
    IS_CONVERTED as converted_check,
    
    CONVERTED_DATE as converted_date,
    
    --Job Information
 
    
    JOB_FUNCTION_CLEANED_C as job_function,
    JOB_LEVEL_CLEANED_C as job_level,
    TITLE as job_title,
    
    PERSONA_C as persona,
 
    ZOOM_INFO_JOB_TITLE_C as zoom_info_job_title,
    ZOOM_INFO_JOB_FUNCTION_C as zoom_info_job_function,
    ZOOM_INFO_MANAGEMENT_LEVEL_C as zoom_info_job_level,
    
    --Company Information
    
    COMPANY as company,
    COMPANY_SIZE_RANGE_CLEANED_C as company_size,
    INDUSTRY_CLEANED_C as lead_industry,
    ZOOM_INFO_COMPANY_NAME_C as zoom_info_company_name,
    ZOOM_INFO_ANNUAL_REVENUE_C as zoom_info_revenue,
    ZOOM_INFO_NO_EMPLOYEES_C as zoom_info_employees,
    ZOOM_INFO_COUNTRY_C as zoom_info_country,
    ACCOUNT_NAME_C as lead_account_id,
    
    --Opportunity Information
    
    CONVERTED_OPPORTUNITY_ID as converted_opportunity_id
    
      
  FROM FIVETRAN_DATABASE.SALESFORCE.LEAD
   

),

accounts as (

SELECT
    
    ID as account_id,
    NAME as account_name,
    ANNUAL_REVENUE as annual_revenue,
    NUMBER_OF_EMPLOYEES as number_of_employees,
    INDUSTRY_CLEANED_C as account_industry,
    ZOOMINFO_SUBINDUSTRY_C as zoominfo_sub_industry,
    SALES_REGION_C as sales_region,
    TYPE as type,
    ABM_TARGET_TYPE_C as abm_target_type,
    TIER_C as account_tier
    
    
FROM FIVETRAN_DATABASE.SALESFORCE.ACCOUNT  
    
),

opportunities AS (

SELECT
    
    ID as opportunity_id,
    NAME as opportunity_name,
    TYPE as opportunity_type,
    ARR_MIRROR_C as arr_mirror,
    FORECASTED_ARR_C as arr_forecast,
    STAGE_NAME as stage,
    CREATED_DATE as opportunity_created_date,
    CLOSE_DATE as opportunity_close_date,
    CLOSED_WON_DATE_C as opportunity_close_won_date,
    CLOSED_LOST_DATE_C as opportunity_close_lost_date
    
FROM FIVETRAN_DATABASE.SALESFORCE.OPPORTUNITY

)


SELECT *
FROM source
LEFT JOIN accounts
ON source.lead_account_id = accounts.account_id
LEFT JOIN opportunities
ON source.converted_opportunity_id = opportunities.opportunity_id