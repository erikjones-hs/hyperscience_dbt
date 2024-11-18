{{ config(materialized='table')}}

with 
    sales_engagement as (
        select
            coalesce(c.email, l.email) as email
            ,t.id as activity_id
            ,t.who_id
            ,l.id as lead_id
            ,c.id as contact_id
            ,t.what_id
            ,t.created_date as activity_date
            ,case when t.task_subtype = 'Email' or t.type_c = 'Email' or t.type = 'Email' then 'Email'
                when t.type_c = 'Call' or t.task_subtype = 'Call' or t.type = 'Call' then 'Call'
                when lower(t.subject) like '%linkedin%' or lower(t.subject) like '%sales%navigator%'
                        or t.activity_type_c = 'LinkedIn Message' then 'LinkedIn Message'
                else 'Other' end as type
            ,t.subject
            ,t.description
            ,case when subject like 'Email:%>>%' then 'Reach Out'
                    when subject like 'LinkedIn Connect: >>%' then 'Reach Out'
                  when subject like 'Email:%<<%' then 'Response' 
                    when lower(subject) like '%inmail%response%' then 'Response'
                  else 'Unknown' end as activity_direction
        from {{ ref('stg_sf_task')}} t
        left join {{ ref('int_sf_lead')}} l
            on t.who_id = l.id
        left join {{ ref('int_sf_contact')}} c
            on t.who_id = c.id
        where
            (t.task_subtype = 'Email' or t.type_c = 'Email' or t.type = 'Email'
                    or t.type_c = 'Call' or t.task_subtype = 'Call' or t.type = 'Call'
                    or lower(t.subject) like '%linkedin%' or lower(t.subject) like '%sales%navigator%'
                        or t.activity_type_c = 'LinkedIn Message')
    )
    ,sales_meetings as (
            select
                coalesce(c.email, l.email) as email
                ,e.id as activity_id
                ,e.who_id
                ,l.id as lead_id
                ,c.id as contact_id
                ,e.what_id
                ,e.activity_date_time as activity_date
                ,'Meeting' as type
                ,e.subject
                ,e.description
                ,'Response' as activity_direction
            from {{ ref('stg_sf_event')}} e
            left join {{ ref('int_sf_lead')}} l
                on e.who_id = l.id
            left join {{ ref('int_sf_contact')}} c
                on e.who_id = c.id
            where
                e.is_child = false -- excludes event updates and adjustments
            qualify
                row_number() over (partition by e.who_id, e.activity_date_time order by e.created_date desc, e.what_id desc) = 1
                    -- excludes multiple event entries from updates or when there are multiple rows due to account and opp being in what_id fields
    )
    ,sales_activities as (
        select * from sales_engagement
        union all
        select * from sales_meetings
    )

select 
    a.email
    ,a.activity_id
    ,a.who_id
    ,a.lead_id
    ,a.contact_id
    ,hc.id as hubspot_contact_id
    ,a.what_id
    ,a.activity_date
    ,a.type
    ,a.subject
    ,a.description
    ,a.activity_direction
from sales_activities a
left join {{ ref('int_hubspot_contacts')}} hc
    on a.email = hc.email
order by activity_date
