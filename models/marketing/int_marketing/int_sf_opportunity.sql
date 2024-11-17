with
    first_opp_contact as (
        select
            opportunity_id,
            contact_id
        from
            {{ ref('stg_sf_opportunity_contact_role')}}
        qualify
            row_number() over (partition by opportunity_id order by created_date) = 1
    )
    ,opps_with_attribution_contact_id as(
        select
            o.*
            ,coalesce(fc.contact_id, o.sales_loft_1_primary_contact_c, o.contact_id) as attribution_contact_id
        from {{ ref('stg_sf_opportunity')}} o
        left join first_opp_contact fc
            on o.id = fc.opportunity_id
    )

select
    oa.*
    ,c.email as attribution_contact_email
    ,hc.id as attribution_hubspot_contact_id
    ,row_number() over (partition by oa.attribution_contact_id order by oa.created_date) as attribution_contact_opp_number
    ,hc.acquisition_channel_type
    ,hc.acquisition_channel
    ,hc.acquisition_channel_detail
    ,hc.acquisition_channel_campaign
    ,hc.acquisition_channel_keyword
    ,hc.lead_creation_source_type
    ,hc.lead_create_source as lead_creation_source
    ,hc.lead_create_source_detail as lead_creation_source_detail
from opps_with_attribution_contact_id oa
left join {{ ref('int_sf_contact')}} c
    on oa.attribution_contact_id = c.id
left join {{ ref('int_hubspot_contacts')}} hc
    on c.email = hc.email
