-- Model to set CRM Created Date as the first date this individual is seen across tools / objects

select
    c.id
    ,c.email
    ,least(
        coalesce(c.createdate, '9999-12-31')
        ,coalesce(l.created_date, '9999-12-31')
        ,coalesce(co.created_date, '9999-12-31')
        ) as crm_created_date
    ,c.createdate as hubspot_created_date
    ,l.created_date as sfdc_lead_created_date
    ,co.created_date as sfdc_contact_created_date
from {{ ref('int_hubspot_contacts')}} c
left join {{ ref('int_sf_lead')}} l
    on c.email = l.email
left join {{ ref('int_sf_contact')}} co
    on c.email = co.email
order by c.createdate desc