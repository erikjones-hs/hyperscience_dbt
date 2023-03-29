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
'0061R00000uLcG2QAK', /* End date adjustment because of open negotiations. Morgan Stanley 148.5k */ 
'0061R0000137hXuQAI', /* End date adjustment because of open negotiations. FIS Global 130k */ 
'0061R000014uXZrQAM', /* End date adjustment because of open negotiations. MPower 99k */
'0061R000014vAD7QAM', /* End date adjustment because of open negotiatios. Mercury Insurance 225k */
'0061R0000136ZbBQAU', /* End date adjustment because of open negotiatios. Reveal 8.8k */
'0061R000014wHDFQA2',  /* End date adjustment because of open negotiatios. Mckinsey 150k */
'0061R000013edS8QAI', /* End date adjustment because of open negotiatios. Amex 275k */
'0061R000010QozNQAS', /* End date adjustment because of open negotiations. Spark Theraputics 8k */
'0061R000016kGCyQAM' /* End date adjustment because of open negotiations. WRK 75k */
)
)

select * from opp_ids