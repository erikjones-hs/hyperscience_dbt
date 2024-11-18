{{ config(materialized='table')}}

with
    first_attributed_opportunities as (
        select
            *
        from {{ ref('int_sf_opportunity')}}
        where
            attribution_contact_opp_number = 1
    )
    ,sales_activity_stage_data as (
        select
            email
            ,hubspot_contact_id
            ,min(activity_date) as initial_reach_out_date
            ,min(case when activity_direction = 'Response' then activity_date else null end) as initial_response_date
        from {{ ref('int_sf_sales_activities')}}
        group by email, hubspot_contact_id
    )


select
    hc.email
    ,hc.id as hubspot_contact_id
    ,hc.sfdc_lead_id as salesforce_lead_id
    ,hc.sfdc_contact_id as salesforce_contact_id
 --   ,created_date
    ,fo.id as converted_opportunity_id
    ,fo.net_new_arr_forecast_c as pipeline
    ,fo.stage_1_date_c
    ,fo.stage_2_date_c
    ,fo.stage_3_date_c
    ,fo.stage_4_date_c
    ,fo.stage_5_date_c
    ,case when fo.is_won = true and fo.is_closed = true then fo.close_date else null end as closed_won_date
    ,case when fo.stage_1_date_c is not null then 1 else 0 end as stage_1_opp
    ,case when fo.stage_2_date_c is not null then 1 else 0 end as stage_2_opp
    ,case when fo.stage_3_date_c is not null then 1 else 0 end as stage_3_opp
    ,case when fo.stage_4_date_c is not null then 1 else 0 end as stage_4_opp
    ,case when fo.stage_5_date_c is not null then 1 else 0 end as stage_5_opp
    ,case when fo.is_won = true and fo.is_closed = true then 1 else 0 end as closed_won_opp
    ,case when fo.is_won = true and fo.is_closed = true then fo.net_new_arr_forecast_c else 0 end closed_won_amount
    ,coalesce(sa.initial_reach_out_date, sa.initial_response_date, fo.created_date) as reaching_out_date
    ,coalesce(sa.initial_response_date, fo.created_date) as engaged_date
    ,fo.created_date as opp_conversion_date
    ,case when sa.initial_reach_out_date is not null then 1 
            when fo.id is not null then 1 else 0 end as is_reaching_out_lead
    ,case when sa.initial_response_date is not null then 1 
            when fo.id is not null then 1 else 0 end as is_engaged_lead
    ,case when fo.id is not null then 1 else 0 end as converted_to_opp_lead
from {{ ref('int_hubspot_contacts')}} hc
left join first_attributed_opportunities fo
    on hc.sfdc_contact_id = fo.attribution_contact_id
left join sales_activity_stage_data sa
    on hc.id = sa.hubspot_contact_id