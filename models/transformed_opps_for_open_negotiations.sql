{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

/* All OPP IDs That have been transformed b/c they are still negotiating renewals */
with opp_ids as (
select distinct opp_id
from {{ref('arr_opp_history')}}
where opp_id in (
'0061R0000137by9QAA', /* End date adjustment because of open negotiations. AIG 555k */
'0061R0000137kdCQAQ', /* End date adjustment because of open negotiations. Unum 625k */
'0061R0000135gO1QAI', /* End date adjustment because of open negotiations. Accerta 89.4k */
 '0061R000014wIeUQAU' /* End date adjustment because of open negotiations. SSA W2 950k */
)
)

select * from opp_ids