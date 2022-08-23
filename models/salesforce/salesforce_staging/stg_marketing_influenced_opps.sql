
select 

    distinct bizible_2_opportunity_c as marketing_influenced_opportunity_id

from {{ source('salesforce', 'bizible_2_bizible_attribution_touchpoint_c') }}
where is_deleted = false