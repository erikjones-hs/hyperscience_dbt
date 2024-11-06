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
from opps_with_attribution_contact_id oa
left join {{ ref('int_sf_contact')}} c
    on oa.attribution_contact_id = c.id
