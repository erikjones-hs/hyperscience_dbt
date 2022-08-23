
select 

    opportunity_id,
    max(case when new_value = 'Value/Fit' then 1 else 0 end) as had_value_fit,
    max(case when new_value = 'TDD' then 1 else 0 end) as had_tdd,
    max(case when new_value = 'EB Go/No Go' then 1 else 0 end) as had_eb_go_no_go,
    max(case when new_value = 'TVE' then 1 else 0 end) as had_tve,
    max(case when new_value = 'EB Revisit' then 1 else 0 end) as had_eb_revisit,
    max(case when new_value = 'Negotiate and Close' then 1 else 0 end) as had_negotiate_and_close

from {{ source('salesforce', 'opportunity_field_history') }}
where field = 'StageName'
group by 1 