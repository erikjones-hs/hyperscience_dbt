select
    id
    ,name
    ,created_date
    ,close_date
    ,stage_name
    ,stage_2_date_c
    ,net_new_arr_forecast_c
    ,revenue_type_c
    ,is_closed
    ,is_won
    ,case when created_date < '2024-09-01' then acquisition_channel_c
          else latest_channel_c end as channel
    ,case when created_date < '2024-09-01' then acquisition_channel_detail_c
          else latest_channel_detail_c end as channel_detail
    ,date_trunc('month', created_date::date) as created_month
    ,date_trunc('month', stage_2_date_c::date) as stage_2_month
    ,date_trunc('month', close_date) as close_month
    ,concat(
        case when month(created_date) in (1,2) then year(created_date) - 1
             else year(created_date) end
        ,' '
        ,case when month(created_date) in (3,4,5) then 'Q1'
              when month(created_date) in (6,7,8) then 'Q2'
              when month(created_date) in (9,10,11) then 'Q3'
              else 'Q4' end
    ) as created_fiscal_quarter
    ,concat(
        case when month(stage_2_date_c) in (1,2) then year(stage_2_date_c) - 1
             else year(stage_2_date_c) end
        ,' '
        ,case when month(stage_2_date_c) in (3,4,5) then 'Q1'
              when month(stage_2_date_c) in (6,7,8) then 'Q2'
              when month(stage_2_date_c) in (9,10,11) then 'Q3'
              else 'Q4' end
    ) as stage_2_fiscal_quarter
    ,concat(
        case when month(close_date) in (1,2) then year(created_date) - 1
             else year(close_date) end
        ,' '
        ,case when month(close_date) in (3,4,5) then 'Q1'
              when month(close_date) in (6,7,8) then 'Q2'
              when month(close_date) in (9,10,11) then 'Q3'
              else 'Q4' end
    ) as close_fiscal_quarter
from {{ ref('int_sf_opportunity')}}