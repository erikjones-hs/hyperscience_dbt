with data as (

select 

member_id,
ifnull(contact_id, lead_id) as person_id,
account_id,
opp_id,
opp_arr,
opp_created_dte,
b.created_date as engagement_date,
campaign_type,
campaign_name,

case
  when 
    datediff('day', opp_created_dte, engagement_date) >= -365 
    and datediff('day', opp_created_dte, engagement_date) < 0 
    then 'Influenced'
  when
    datediff('day', opp_created_dte, engagement_date) < -365
    then 'Not Influenced'
  when engagement_date > (
    case
      when opp_stage_name = 'Closed Won' then opp_closed_won_dte
      when opp_stage_name = 'Closed Lost' then opp_dq_dte
      else opp_close_dte
    end
  ) then 'Not Influenced'
  else 'Accelerated'
end as influence_type

from {{ ref('dim_opportunity') }} a
left join {{ ref('dim_campaign_member_accounts') }} b using (account_id)
where b.created_date is not null
and account_id != '0011R000021BlB0QAK'
and account_id is not null

)

select
    *,
    concat(member_id, person_id, opp_id) as unique_id
from 
    data
where influence_type != 'Not Influenced'