
select
    id as bizible_person_id,
    bizible_2_contact_c as contact_id,
    bizible_2_lead_c as lead_id
from {{ source('salesforce', 'bizible_2_bizible_person_c') }}





