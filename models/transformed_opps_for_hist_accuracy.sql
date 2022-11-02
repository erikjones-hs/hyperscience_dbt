{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

/* All OPP IDs That have been transformed for histiorical accuracy purposes */
with opp_ids as (
select distinct opp_id
from {{ref('arr_opp_history')}}
where opp_id in (
'0063600000X36zWAAR', 
'0061R00000m11f1QAA', 
'0061R00000tE0SIQA0', 
'0063600000eMMZeAAO', 
'0061R00000pmValQAE', 
'0063600000FJwn2AAD', 
'0063600000iS5wbAAC', 
'0061R00000r6r1iQAA',  /* Adjusting the end date for CRL because they are still a customer, but with no committed revenue */
'0061R00000yFonNQAS',  /* Adjusting the end date for MetaSource because they are still a customer, but with no committed revenue */
'0061R00000zAI8KQAW',  /* Adjusting the end date for Virginia DMV because they are still a customer, but with no committed revenue */
'0061R00000yEQVgQAO',  /* Adjusting the end date for GDIT-VA because they are still a customer, but with no committed revenue */
'0061R00000r83epQAA', 
'0061R00000wKxG5QAK', 
'0061R00000uJr07QAC', 
'0061R000010tCbNQAU', 
'0061R000010tCZvQAM', 
'0061R00000oERITQA4', 
'0063600000M73LuAAJ', 
'0063600000dsPyXAAU', 
'0061R00000kRNPDQA4', 
'0063600000dsPsjAAE', 
'0061R00000zAM8wQAG', 
'0061R00000tFLB3QAO', 
'0061R00000uL8ylQAC',  /* Adjusting the end date for PMP because they are still a customer, but with a 1 year free contract period */
'0061R00000zAjoeQAC', 
'0061R000010PVABQA4', 
'0061R00000zBqNRQA0', 
'0061R000013fGLrQAM', 
'0061R00000wJkMuQAK', 
'0061R0000137saUQAQ', 
'0061R000013fGTbQAM',  /* Updated end date because it is incorect in SFDC. divvyDose 180k */
'0061R00000zD2sTQAS',  /* End date adjusted per Kristen and Finance ARR Google Sheet. Conduent 280k */ 
'0061R000010t71kQAA',  /* Customer no longer is paying. Close this out in Jan. per FP&A. Sience SAS 41.65k */
'0061R000014uXZrQAM',  /* Updated MPOWER end date because it is incorrect in SFDC */
'0061R00000yElHXQA0',  /* Customer Churned in Feb, per FP&A. Department of Treasury 87.5k */
'0061R00000zAlU8QAK',  /* Opportunity churned in March, per Kristen. AMEX 323k */
'0061R0000136hnzQAA',  /* Customer churned in Feb. per Kristen. AXA Churn. 35k */ 
'0061R000014vAD7QAM',  /* Adjusting end date because it is incorrect in SFDC */
'0061R00000r7xPhQAI',  /* Customer churned. Close this out in Feb. per FP&A. DISA 64.3k */
'0061R0000137tYlQAI',  /* Customer churned. Close this out in Mar. per FP&A. Record Connect 239k */ 
'0061R0000137kNxQAI',  /* Customer Churned in April per FP&A. State of Texas 402.5k total. 17.5k opp */
'0061R000010ujZ5QAI',  /* Adjusting end date because of new contract with expansion that starts in June */
'0061R00000zD2sxQAC',  /* End date adjustment because renewal date is incorrect in SFDC. Conduent 1.98M */
'0061R0000137ijiQAA',  /* End date adjustment due to negotiated end of contract. Johnson Law Group 25k */
'0061R0000137jqkQAA',  /* Adjusting end date because wrong in Salesforce. QAI 35k */
'0061R00000zDCt9QAG',  /* End date adjustment because renewal date was wrong in snapshot */
'0061R000010QadCQAS',  /* End date adjustment to account for amended contract. Philadelphia Insureance Company 300k */
'0061R0000137jsqQAA',  /* Adjusting End Date for historical accuracy. Pac Life 330k */
'0061R000010O65hQAC',  /* Adjusting End Date for historical accuracy. First American Financial 1M */
'0061R0000137hQzQAI',  /* End date adjustment for historical accuracy. Allstate 15k */
'0061R00001A4pwsQAB',  /* End date adjustment because it is wrong in SFDC. Ascensus 216k */
'0061R000010OgSrQAK',  /* End date adjustment for historical accuracy. GAIG 180k */
'0061R000013fHgQQAU',  /* End date adjustment for historical accuracy. IRS phase 2 */
'0061R0000137hOKQAY',  /* End date adjustment for historical accuracy. SSA DeDupe 1.9M */
'0061R000013flkIQAQ',  /* End date adjustment for historical accuracy. VBA IBM 2.3M */
'0061R000010tH9RQAU'  /* End date adjustment for historical accuracy. VA VICCS 1.2M */
)
)

select * from opp_ids