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
'006Dm000002dNKAIA2', /* US Navy. Updated end date because wrong in snapshot */ 
'0061R00001A6cAuQAJ', /* End date adjustment because of open negotiations. QBE 375k */
'0061R0000137Uv3QAE',  /* USAC */
'0061R00001CxtHbQAJ', /* USAC 6.4k */
'0061R000019RV3tQAG', /* Dept of VA 7.6M*/
'006Dm000002cWWgIAM', /* UNUM 500k */
'006Dm000002fQa8IAE', /* Accerta. 154.7k */
'0061R000019QdXAQA0', /* Federated 130k */
'006Dm000002dwQEIAY' /* DivvyDose 130k */    
)
)

select * from opp_ids