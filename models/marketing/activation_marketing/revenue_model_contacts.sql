with
    sales_accepted_lead_data as (
        select
            hubspot_contact_id
            ,min(activity_date) as initial_meeting_date
        from {{ ref('int_sf_sales_activities')}}
        where
            type = 'Meeting'
        group by hubspot_contact_id
    )

select  
    h.id
    ,h.sfdc_contact_id
    ,h.crm_created_date
    ,h.is_reached_out_lead
    ,h.is_engaged_lead
    ,case when sal.initial_meeting_date is not null then 1 else 0 end as is_sal
    ,sal.initial_meeting_date as sal_date
    ,h.acquisition_channel
    ,h.acquisition_channel_detail
    ,date_trunc('month', h.crm_created_date::date) as created_month
    ,date_trunc('month', sal.initial_meeting_date) as sal_month
    ,concat(
        case when month(h.crm_created_date) in (1,2) then year(h.crm_created_date) - 1
             else year(h.crm_created_date) end
        ,' '
        ,case when month(h.crm_created_date) in (3,4,5) then 'Q1'
              when month(h.crm_created_date) in (6,7,8) then 'Q2'
              when month(h.crm_created_date) in (9,10,11) then 'Q3'
              else 'Q4' end
    ) as created_fiscal_quarter
from {{ ref('int_hubspot_contacts')}} h
left join sales_accepted_lead_data sal
    on h.id = sal.hubspot_contact_id