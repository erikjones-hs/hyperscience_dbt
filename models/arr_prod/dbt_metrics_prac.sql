select * 
from {{
    metrics.calculate(
        metric('arr_qtr'),
        grain='month',
        secondary_calculations=[
            metrics.period_over_period(comparison_strategy="ratio", interval=3, alias = "qtr_over_qtr")
        ]
    )
}}
where arr_qtr > 0
order by date_month asc


