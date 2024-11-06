select
    cm.id
    ,cm.email
    ,cm.lead_id
    ,cm.contact_id
    ,hc.id as hubspot_contact_id
    ,cm.created_date
    ,cm.status
    ,cm.* exclude (id, email, lead_id, contact_id, created_date, status)
from {{ ref('stg_sf_campaign_member')}} cm
left join {{ ref('stg_hubspot_contacts')}} hc
    on cm.email = hc.email
    and hc.email is not null