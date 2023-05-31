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
'0061R0000136ZbBQAU', /* End date adjustment because of open negotiatios. Reveal 8.8k */
'0061R000014vRzBQAU', /* Mision Underwriters 145k */
'0061R000014wKFCQA2', /* Teknei 50k */
'0061R000014wNt6QAE', /* ADP 9.6k */
'0061R000014wI4nQAE', /* Consiel 16k */
'0061R000014xeQwQAI'  /* BenefitMall 13.3k */
)
)

select * from opp_ids