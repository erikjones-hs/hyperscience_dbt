{{ config(materialized='table')}}

with
    post_campaign_activities as (
        select
            cm.id
            ,sum(case when a.activity_date >= cam.start_date then 1 else 0 end) as post_campaign_activities
            ,sum(case when a.activity_date >= cam.start_date and a.type = 'Meeting' 
                        then 1 else 0 end) as post_campaign_meetings
            ,min(case when a.activity_date >= cam.start_date and a.type = 'Meeting' 
                        then a.activity_date else null end) as first_post_campaign_meeting_date
        from {{ ref('int_sf_campaign_member')}} cm
        left join {{ ref('int_sf_campaign')}} cam
            on cm.campaign_id = cam.id
        left join {{ ref('int_sf_sales_activities')}} a
            on cm.email = a.email
        group by
            cm.id
    )
    ,post_campaign_opportunities as (
        select
            cm.id
            ,sum(case when o.created_date >= cam.start_date then 1 else 0 end) as post_campaign_opportunities
            ,min(case when o.created_date >= cam.start_date then 1 else 0 end) as initial_post_campaign_opportunity_date
        from {{ ref('int_sf_campaign_member')}} cm
        left join {{ ref('int_sf_campaign')}} cam
            on cm.campaign_id = cam.id
        left join {{ ref('int_sf_opportunity')}} o
            on cm.email = o.attribution_contact_email
        group by
            cm.id
    )

select
    cm.email
    ,cm.id
    ,cm.campaign_id
    ,cm.status
    ,cm.created_date as campaign_added_date
    ,pca.post_campaign_activities
    ,pca.post_campaign_meetings
    ,pca.first_post_campaign_meeting_date
    ,pco.post_campaign_opportunities
    ,pco.initial_post_campaign_opportunity_date
from {{ ref('int_sf_campaign_member')}} cm
left join post_campaign_activities pca
    on cm.id = pca.id
left join post_campaign_opportunities pco
    on cm.id = pco.id
