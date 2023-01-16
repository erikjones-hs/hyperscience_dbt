select * 
from {{
    metrics.calculate(
        metric('arr_month'),
        grain='month',
        secondary_calculations=[
            metrics.period_over_period(comparison_strategy="ratio", interval=1, alias = "month_over_month")
        ]
    )
}}
order by date_month asc


