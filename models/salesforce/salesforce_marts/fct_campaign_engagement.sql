
    select 
        member_id,
        contact_id,
        lead_id,
        member_created_date as created_date,
        case 
            when campaign_name like '%Demo%' then 'Demo'
            when campaign_name like '%C_report%' then 'Report'
            when campaign_name like '%C_ebook%' then 'eBook'
            when campaign_name like '%Contact%' then 'Contact Us Request'
            when campaign_name like '%DR%' then 'Drift'
            when campaign_type = 'Website' then 'Web Content'
            else campaign_type
        end as campaign_type,
        campaign_name
        from {{ ref('dim_campaign_members_with_campaign') }}
        where first_responded_date is not null
        and (campaign_type != 'Operational' or campaign_name like '% TP %')
