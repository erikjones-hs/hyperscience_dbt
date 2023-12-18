with data as (

  select 
  
  *,

  -- multi-touch attribution

  div0(

    1,
    count(unique_id) over (partition by opp_id)

  ) as mt_model

  from {{ ref('mart_account_opportunity_influence') }}
  order by engagement_date desc

),

first_touch_attribution as (

  select 

  unique_id,
  iff(dense_rank() over (partition by opp_id order by engagement_date asc, person_id) = 1, 1, 0) as ft_model

  from {{ ref('mart_account_opportunity_influence') }}
  where influence_type = 'Influenced'

),

last_touch_attribution as (

  select 

  unique_id,
  iff(dense_rank() over (partition by opp_id order by engagement_date desc, person_id) = 1, 1, 0) as lt_model
  
  from {{ ref('mart_account_opportunity_influence') }}
  where influence_type = 'Influenced'

),

models as (

    select *
    from data 
    left join first_touch_attribution 
    using (unique_id)
    left join last_touch_attribution
    using (unique_id)

)

select
    unique_id,
    person_id,
    account_id,
    opp_id,
    iff(opp_stage_name in ('Closed Won', 'Closed Lost'), opp_stage_name, 'Active') as opp_status,
    engagement_date,
    campaign_type,
    campaign_name,
    influence_type,
    mt_model * opp_arr as mt_model_arr,
    ft_model * opp_arr as ft_model_arr,
    lt_model * opp_arr as lt_model_arr,
    mt_model * 1 as mt_model_count,
    iff(ft_model > 0, opp_id, null) as ft_opp_id,
    iff(lt_model > 0, opp_id, null) as lt_opp_id

from models
left join {{ ref('dim_opportunity') }}
using (opp_id)