{{ config
(
    materialized ='incremental',
    database = 'PROD',
    schema = 'CX'
)
}}

with cx_forecast as (
select *,
to_date(date) as dte_ordered,
to_date(current_date) as date_ran
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."CX_FORECAST_V_1"
order by dte_ordered asc
)

select * from cx_forecast

{% if is_incremental() %}

  where date_ran >= (select max(date_ran) from {{ this }})

{% endif %}