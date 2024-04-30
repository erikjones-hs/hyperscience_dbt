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
'0061R00000uL8ylQAC', /* End date adjustment because of open negitotaions. PMP $0 */
'0061R00000yFonNQAS', /* ENd date adjustment because of open negotiations. Metasource $0 */    
'0061R00001A4rKQQAZ', /* Kovack. Transformed for renewal date purposes only */
'0061R000019QwEZQA0', /* End date adjustment because of open negotiations. DLA 40k */
'0061R000014yHlcQAE', /* End date adjustment because of open negotiations. Transflo 1.3M */
'0061R00000wLePIQA0', /* End date adjustment because of open negotiations. FirstRand 500k */
'0061R00001BAufLQAT', /* End date adjustment because of open negotiations. MIssion Underwriting 145k */
'0061R000014wK2vQAE', /* End date adjustment because of open negotiations. NRO 30.3k */
'0061R00001A6cAuQAJ', /* End date adjustment because of open negotiations. QBE 375k */
'0061R00001A6BeZQAV'  /* End date adjustment because of open negotiations. Guarddian Life 250k */   
)
)

select * from opp_ids