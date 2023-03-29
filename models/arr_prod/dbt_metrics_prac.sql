select * 
from {{
    metrics.calculate(
        metric('arr_customers'),
        grain='month'
    )
}}
order by date_month asc


