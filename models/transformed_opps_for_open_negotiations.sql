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
'0061R000016kGCyQAM', /* End date adjustment because of open negotiations. WRK 75k */
'0061R0000135VUDQA2', /* QBE 300k */
'0061R000014vRzBQAU', /* Mision Underwriters 145k */
'0061R000014wKFCQA2', /* Teknei 50k */
'0061R000013fuawQAA'  /* IRS 97.5k */
)
)

select * from opp_ids