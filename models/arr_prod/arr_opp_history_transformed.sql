{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

/* Pulling in raw SFDC data from arr_opp_history model*/
with raw_data as (
select * from {{ref('arr_opp_history')}}
),

/* transforming start dates to start months */
/* adjusting start and end dates to match historical finance data from ARR Google Sheet */
/* adjusting arr to match historical finance data from ARR Google Sheet */
raw_data_transformed as (
select 
CASE WHEN account_id = '0011R000026iP6rQAE' then '0013600001iRke2AAC' 
     WHEN account_id = '0013600001hWo0yAAC' then '0011R00002HKzaCQAT'
     when account_id = '0011R00002UxEwKQAV' then '0011R00002pmZ7LQAU'
     when account_id = '001Dm000002jMVdIAM' then '00136000009czySAAQ'
     else account_id end as account_id,
CASE WHEN account_name = 'TD Ameritrade' then 'Charles Schwab' 
     WHEN account_name = '8053580156557' then 'Department of Justice' 
     WHEN account_name = '8780895197581' then 'Mathematica, Inc.'
     WHEN account_name = 'Tokio Marine HCC' then 'Philadelphia Insurance Companies'
     WHEN account_name = 'Great American Insurance Group' then 'Great American Insurance Company'
     WHEN account_name = 'IBM' then 'Department of Veterans Affairs'
     when account_name = 'Momentum Metropolitan Holdings Limited' then 'Momentum'
     when account_name = 'ALMAC.' then 'ALMAC'
     when account_name = 'Mutual of Omaha' then 'Mutual of Omaha Insurance Company'
     when account_name = 'AIG (American International Group, Inc)' then 'SAFG Technologies, LLC'
     when account_name = 'Clinical Reference Laboratory' then 'Clinical Reference Laboratory, Inc.'
     else account_name end as account_name,
opp_id,
opp_name,
opp_revenue_type,
CASE WHEN opp_id = '0063600000X36zWAAR' then to_date('2020-07-01')
     WHEN opp_id = '0061R00000m11f1QAA' then to_date('2020-07-01')
     WHEN opp_id = '0061R00000tE0SIQA0' then to_date('2021-06-01')
     WHEN opp_id = '0063600000eMMZeAAO' then to_date('2019-12-01')
     WHEN opp_id = '0061R00000pmValQAE' then to_date('2020-06-17')
     WHEN opp_id = '0063600000FJwn2AAD' then to_date('2020-07-31')
     WHEN opp_id = '0063600000iS5wbAAC' then to_date('2020-01-01')
     WHEN opp_id = '0061R00000yEQVgQAO' then to_date('2023-11-15') /* Adjusting the end date for GDIT-VA because they are still a customer, but with no committed revenue */
     WHEN OPP_ID = '0061R00000r83epQAA' THEN TO_DATE('2020-09-15')
     WHEN opp_id = '0061R00000wKxG5QAK' THEN to_date('2020-08-15')
     WHEN opp_id = '0061R00000uJr07QAC' THEN to_date('2021-08-15')
     WHEN opp_id = '0061R000010tCbNQAU' THEN to_date('2020-10-15')
     WHEN opp_id = '0061R000010tCZvQAM' THEN to_date('2021-10-15')
     WHEN opp_id = '0061R00000oERITQA4' THEN to_date('2022-10-15')
     WHEN opp_id = '0063600000M73LuAAJ' then to_date('2020-02-15')
     WHEN opp_id = '0063600000dsPyXAAU' then to_date('2019-10-15')
     WHEN opp_id = '0061R00000kRNPDQA4' then to_date('2020-10-15')
     WHEN opp_id = '0063600000dsPsjAAE' then to_date('2020-08-15')
     WHEN opp_id = '0061R00000zAM8wQAG' then to_date('2021-08-15')
     WHEN opp_id = '0061R00000tFLB3QAO' then to_date('2021-11-15')
     WHEN opp_id = '0061R00000zAjoeQAC' then to_date('2021-10-15')
     WHEN opp_id = '0061R000010PVABQA4' then to_date('2021-10-15')
     WHEN opp_id = '0061R00000zBqNRQA0' then to_date('2021-11-15')
     WHEN opp_id = '0061R00000wJkMuQAK' then to_date('2021-12-15')
     when opp_id = '0061R0000137saUQAQ' then to_date('2022-06-15') 
     WHEN opp_id = '0061R000013fGTbQAM' then to_date('2022-11-23') /* Updated end date because it is incorect in SFDC. divvyDose 180k */
     WHEN opp_id = '0061R00000zD2sTQAS' then to_date('2021-12-15') /* End date adjusted per Kristen and Finance ARR Google Sheet. Conduent 280k */ 
     WHEN opp_id = '0061R000010t71kQAA' then to_date('2022-01-15') /* Customer no longer is paying. Close this out in Jan. per FP&A. Sience SAS 41.65k */
     when opp_id = '0061R00000yElHXQA0' then to_date('2022-02-15') /* Customer Churned in Feb, per FP&A. Department of Treasury 87.5k */
     when opp_id = '0061R00000zAlU8QAK' then to_date('2022-03-15') /* Opportunity churned in March, per Kristen. AMEX 323k */
     when opp_id = '0061R0000136hnzQAA' then to_date('2022-02-15') /* Customer churned in Feb. per Kristen. AXA Churn. 35k */ 
     when opp_id = '0061R00000r7xPhQAI' then to_date('2022-02-15') /* Customer churned. Close this out in Feb. per FP&A. DISA 64.3k */ 
     when opp_Id = '0061R0000137tYlQAI' then to_date('2022-03-15') /* Customer churned. Close this out in Mar. per FP&A. Record Connect 239k */
     when opp_id = '0061R0000137kNxQAI' then to_date('2022-04-15') /* Customer Churned in April per FP&A. State of Texas 402.5k total. 17.5k opp */
     when opp_id = '0061R000010ujZ5QAI' then to_date('2022-06-15') /* Adjusting end date because of new contract with expansion that starts in June */
     when opp_id = '0061R0000137ijiQAA' then to_date('2022-06-29') /* End date adjustment due to negotiated end of contract. Johnson Law Group 25k */
     when opp_id = '0061R0000137jqkQAA' then to_date('2022-08-19') /* Adjusting end date because wrong in Salesforce. QAI 35k */
     when opp_id = '0061R00000zDCt9QAG' then to_date('2024-05-15') /* End date adjustment because renewal date was wrong in snapshot */
     when opp_id = '0061R000010QadCQAS' then to_date('2027-03-15') /* End date adjustment to account for amended contract. Philadelphia Insureance Company 300k */
     when opp_id = '0061R0000137jsqQAA' then to_date('2022-08-15') /* Adjusting End Date for historical accuracy. Pac Life 330k */
     when opp_id = '0061R000010O65hQAC' then to_date('2022-08-15') /* Adjusting End Date for historical accuracy. First American Financial 1M */
     when opp_id = '0061R0000137hQzQAI' then to_date('2022-10-15') /* End date adjustment for historical accuracy. Allstate 15k */
     when opp_id = '0061R000010OgSrQAK' then to_date('2022-11-15') /* End date adjustment for historical accuracy. GAIG 180k */
     when opp_id = '0061R000013fHgQQAU' then to_date('2022-10-15') /* End date adjustment for historical accuracy. IRS phase 2 */
     when opp_id = '0061R0000137hOKQAY' then to_date('2022-09-15') /* End date adjustment for historical accuracy. SSA DeDupe 1.9M */
     when opp_id = '0061R000013flkIQAQ' then to_date('2022-10-15') /* End date adjustment for historical accuracy. VBA IBM 2.3M */
     when opp_id = '0061R000010tH9RQAU' then to_date('2022-10-15') /* End date adjustment for historical accuracy. VA VICCS 1.2M */
     when opp_id = '0061R000013gijQQAQ' then to_date('2022-11-15') /* End Date Adjustment per FP&A. Not paying. MindMap 150k */
     when opp_id = '0061R000016mzrWQAQ' then to_date('2022-11-15') /* End Date Adjustment per FP&A. Not paying. Featsystems 60k */
     when opp_id = '0061R0000137scfQAA' then to_date('2022-11-15') /* End Date Adjustment per FP&A. Not Paying. Cogent 95k */ 
     when opp_id = '0061R000014wIeUQAU' then to_date('2022-10-15') /* End date adjustment for historical accuracy. SSA W2 950k */
     when opp_id = '0061R00000zBWKMQA4' then to_date('2022-11-15') /* End date adjustment due to upsell. Momentum 330.67k */
     when opp_id = '0061R000013fGLrQAM' then to_date('2022-12-15') /* End date adjustment for historical accuracy. Legal and General 315k */
     when opp_id = '0061R00001382KHQAY' then to_date('2022-12-01') /* End date adjustment because new opp for lower ARR due to exchange rates. Almac. 168k */
     when opp_id = '0061R000014vnNlQAI' then to_date('2023-01-15') /* End Date adjustment because they are not paying. I3systems 120k */
     when opp_id = '0061R000013f0rkQAA' then to_date('2023-02-15') /* End date adjustment because it is wrong in SFDC. State of CO 214k */
     when opp_id = '0061R000014vAD7QAM' then to_date('2023-02-15') /* End date adjustment because it is wrong in SFDC. Mercury Insurance 225k */
     when opp_id = '0061R000014uXZrQAM' then to_date('2024-01-15') /* End date adjustment because it is wrong in SFDC. MPower 99k */
     when opp_id = '0061R00001A4rGJQAZ' then to_date('2025-03-09') /* End date adjustment because it is wrong in SFDC. McKinsey 30k */
     when opp_id = '0061R000014wHDFQA2' then to_date('2023-02-15') /* End date adjustment because it is wrong in SFDC. Mckinsey 150k */
     when opp_id = '0061R000013edS8QAI' then to_date('2023-04-15') /* End date adjustment because it is wrong in SFDC. Amex 275k */
     when opp_id = '0061R000014wI4bQAE' then to_date('2023-05-02') /* End date adjustment because they churned early. Manulife 375k */
     when opp_id = '0061R000013fuawQAA' then to_date('2023-05-13') /* End date adjustment because of early churn. IRS 97.5k */  
     when opp_id = '0061R000014wI4lQAE' then to_date('2023-09-15') /* End date adjustment because of extended end date. FATCO 75k */
     when opp_id = '0061R00000uINyXQAW' then to_date('2023-07-15') /* End date adjustment bevause it is wrong in SFDC. Fidelity 1.4M */
     when opp_id = '0061R000014xeQwQAI' then to_date('2023-07-15') /* End date adjustment because it is wrong in SFDC. BenefitMall 13.3k */
     when opp_id = '0061R0000135VUDQA2' then to_date('2023-05-01') /* End date adjustment because of early renewal. QBE Australia 300k */ 
     when opp_id = '0061R0000136ZbBQAU' then to_date('2023-06-15') /* End date adjustment because it is wrong in SFDC. Reveal 8.8k */
     when opp_id = '0061R000014wRB4QAM' then to_date('2023-08-15') /* End date adjustment because it is wrong in SFDC. IRS 300k */ 
     when opp_id = '0061R000014wNroQAE' then to_date('2023-09-15') /* End date adjustment because it is wrong in SFDC. Pacific Life 180k */ 
     when opp_id = '006Dm000002cdEUIAY' then to_date('2023-10-15') /* End date adjustment because wrong in SFDC. VA VICCS 1.5M */ 
     when opp_id = '0061R000019QdXAQA0' then to_date ('2024-11-29') /* End date adjustment because it is wrong in SFDC. Federated Mutual 130k */
     when opp_id = '0061R00001A4pwYQAR' then to_date('2023-10-15') /* End date adjustment because it is wrong in SFDC. Unum 690k */
     when opp_id = '0061R000019Qd92QAC' then to_date('2024-10-29') /* End date adjustment because it is wrong in SFDC. Ascensus 225k */
     when opp_id = '0061R000016nZwpQAE' then to_date('2023-11-15') /* End date adjustment because it is wrong in SFDC. VetsEZ 500k */
     when opp_id = '0061R00001A43h0QAB' then to_date('2023-10-15') /* End date adjustment because combined with other IRS opp. IRS 180k */
     when opp_id = '006Dm000004A3YbIAK' then to_date('2023-11-15') /* End date adjustment because combined with another opp. Caada Life 40k */
     when opp_id = '0061R00001A4rKQQAZ' then to_date('2023-11-15') /* End date adjustment because wrong in SFDC. Kovack 35k */
     when opp_id = '0061R00001A4pwxQAB' then to_date('2023-11-15') /* End date adjustment because wrong in SFDC. Accerta 81.9k */
     when opp_id = '0061R00001BAufzQAD' then to_date('2023-09-15') /* End date adjustment because it is wrong in SFDC. Tech Mahindra 125k */ 
     when opp_id = '0061R00001A4pwsQAB' then to_date('2023-10-15') /* End date adjustment because it is wrong in SFDC. Ascensus 216k */
     when opp_id = '0061R00000yFDbcQAG' then to_date('2023-11-15') /* End date adjustment because it is replaced by an upsell. Irish Life 138.8k */
     when opp_id = '0061R00000r6r1iQAA' then to_date('2023-02-15') /* End date adjustment because it is wrong in SFDC. CRL 375k */
     when opp_id = '0061R000014vUKMQA2' then to_date('2023-12-15') /* End date adjustment because of extension. USAF 115k */  
     when opp_id = '0061R000014yeOrQAI' then to_date('2024-01-15') /* End date adjustment because of extension. Mathematica 100k */   
     when opp_id = '0061R00001A4rFVQAZ' then to_date('2023-12-15') /* End date adjustment because of extension. Legal and General 253k */
     when opp_id = '0061R00001A4rFfQAJ' then to_date('2024-02-15') /* End date adjustment because it is wrong in SFDC. Mercury Insuarance 225k */
     when opp_id = '0061R00001A4rIoQAJ' then to_date('2024-01-15') /* End date adjustment because it is wrong in SFDC. Protective Life 120k */
     when opp_id = '0061R000014wKFCQA2' then to_date('2023-05-15') /* End date adjustment because we learned about churn. Teknei 50k */
     when opp_id = '0061R00001BAug4QAD' then to_date('2024-02-15') /* End date adjustment because we learned about churn. Teknei 50k */
     when opp_id = '006Dm000002dhpbIAA' then to_date('2024-02-15') /* End date adjustment because it is wrong in SFDC. CRL 100k */
     when opp_id = '0061R000010sXv7QAE' then to_date('2024-01-01') /* End date adjustment because it is wrong in SFDC */
     when opp_id = '0061R000014wcRjQAI' then to_date('2024-02-15') /* End date adjustment because we learned about churn. PLUS PLatform 30k */
     when opp_id = '0061R000014xuWkQAI' then to_date('2024-02-15') /* End date adjustment because we learned about churn WNS AIS 400k */
     when opp_id = '006Dm000004mRoiIAE' then to_date('2024-01-26') /* End date adjustment because we learned about churn MPower 126k */
     when opp_id = '0061R0000135QZ1QAM' then to_date('2024-02-15') /* End date adjustment because of known churn Umlaut 500k */
     when opp_id = '0061R000010Q9dvQAC' then to_date('2024-03-15') /* End date adjustment because wrong in SFDC. TTC 142.5k */
     when opp_id = '0061R000013fGPFQA2' then to_date('2024-03-15') /* End date adjustment because replaced by expansion opp. MMH 60k */
     when opp_id = '0061R00000yGqH3QAK' then to_date('2024-03-15') /* End date adjustment because of extension. SSA 2.3M */
     when opp_id = '0061R00001A5k8bQAB' then to_date('2024-03-15') /* End date adjustment because of extension. SSA 1.45M */
     when opp_id = '0061R000014wNrtQAE' then to_date('2024-03-15') /* End date adjustment because of extension. SSA 1.93M */
     when opp_id = '006Dm0000047m76IAA' then to_date('2024-03-15') /* End date adjustment because wog in SFDC. Transflo upsell 236k */
     when opp_id = '0061R000014vdYrQAI' then to_date('2024-02-15') /* End date adjustment because of de-book. Hyperautomation 900k */
     when opp_id = '0061R000014yfmYQAQ' then to_date('2024-02-15') /* End date adjustment because of de-book. Pepsi 125k */
     when opp_id = '0061R00001A4vicQAB' then to_date('2025-03-30') /* End date adjustment because it is wrong in SFDC. Conduent 1.98M */
     when opp_id = '0061R00001A3ujGQAR' then to_date('2024-03-15') /* End date adjustment because it is replaced by another opp. Philly Insurance 360k */
     when opp_id = '0061R00000zD2sxQAC' then to_date('2024-04-15') /* End date adjustment because it is wrong in SFDC. Coduent 1.98M */
     when opp_id = '0061R00000uL8ylQAC' then to_date('2024-07-15') /* End date adjustment because of open negitotaions. PMP $0 */
     when opp_id = '0061R00000yFonNQAS' then to_date('2024-07-15') /* ENd date adjustment because of open negotiations. Metasource $0 */
     when opp_id = '0061R000019QwEZQA0' then to_date('2024-05-15') /* End date adjustment because it is wrong in SFDC. DLA 40k */
     when opp_id = '0061R000014yHlcQAE' then to_date('2024-07-15') /* End date adjustment because of open negotiations. Transflo 1.3M */
  --   when opp_id = '0061R00000wLePIQA0' then to_date('2024-06-15') /* End date adjustment because of open negotiations. FirstRand 500k */
   --  when opp_id = '0061R00001BAufLQAT' then to_date('2024-06-15') /* End date adjustment because of open negotiations. MIssion Underwriting 145k */
     when opp_id = '0061R000016kGCyQAM' then to_date('2024-05-15') /* End date adjustment because failed to pay. WRK 75k */
     when opp_id = '0061R00001A6F76QAF' then to_date('2024-05-15') /* End date adjustment because failed to pay. WRk 15k */
     when opp_id = '0061R000014wK2vQAE' then to_date('2024-05-15') /* End date adjustment because of open negotiations. NRO 30.3k */
     when opp_id = '0061R00001A6cAuQAJ' then to_date('2024-07-15') /* End date adjustment because of open negotiations. QBE 375k */
  --   when opp_id = '0061R00001A6BeZQAV' then to_date('2024-06-15') /* End date adjustment because of open negotiations. Guarddian Life 250k */
   --  when opp_id = '0061R00001A5eXkQAJ' then to_date('2024-07-15') /* ADP */
     when opp_id = '0061R000010O3QoQAK' then to_date('2024-07-15') /* CDCR */
     when opp_id = '006Pm000008xmdVIAQ' then to_date('2024-07-15') /* Transflo */  
     when opp_id = '0061R000016nPUDQA2' then to_date('2024-06-15') /* End date adjustment because of early churn. FTI 130k */
     ELSE end_dte_raw end as end_dte,
end_dte_raw,
CASE WHEN opp_id = '0061R00000uINyXQAW' then to_date('2020-08-01')
     WHEN opp_id = '0061R00000uIehuQAC' then to_date('2020-01-01')
     WHEN opp_id = '0061R00000zD2sxQAC' then to_date('2020-12-01')
     WHEN opp_id = '0063600000FJwn2AAD' then to_date('2019-07-01')
     WHEN opp_id = '0061R00000pmValQAE' then to_date('2019-06-15')
     WHEN opp_id = '0061R00000tF1MSQA0' then to_date('2020-10-15')
     WHEN opp_id = '0061R00000zAM8wQAG' then to_date('2020-09-15')
     WHEN opp_id = '0061R00000zDAxAQAW' then to_date('2020-02-01')
     WHEN opp_id = '0063600000X36zWAAR' then to_date('2018-04-01')
     WHEN opp_id = '0063600000kQAyCAAW' then to_date('2018-12-15')
     WHEN opp_id = '0061R00000pkoODQAY' then to_date('2019-08-15')
     WHEN opp_id = '0063600000eMMZeAAO' then to_date('2018-12-15')
     WHEN opp_id = '0063600000dsPsjAAE' then to_date('2018-06-15')
     WHEN opp_id = '0063600000M73LuAAJ' then to_date('2019-02-15')
     WHEN opp_id = '0063600000dsPyXAAU' then to_date('2018-10-15')
     WHEN opp_id = '0061R00000kRNPDQA4' then to_date('2019-10-15')
     WHEN opp_id = '0061R0000137ijiQAA' then to_date('2020-06-30')
     WHEN opp_id = '0061R000013flkIQAQ' then to_date('2021-10-15')
     WHEN opp_id = '0061R000014xeQwQAI' then to_date('2022-01-15')
     WHEN opp_id = '0061R0000135gO1QAI' then to_date('2021-12-15')
     WHEN opp_id = '0061R0000137hXuQAI' then to_date('2022-02-15')
     WHEN opp_id = '0061R0000136ZbBQAU' then to_date('2022-03-15')
     when opp_id = '0061R000014wI4qQAE' then to_date('2022-06-01')
     when opp_id = '0061R000016my8LQAQ' then to_date('2022-06-15')
     when opp_id = '0061R000016jsHbQAI' then to_date('2022-07-15')
     when opp_id = '0061R00001A3ujGQAR' then to_date('2022-03-15')
     when opp_id = '006Dm000002eKjzIAE' then to_date('2022-12-15')
     when opp_id = '0061R00001A4rGJQAZ' then to_date('2023-02-15')
     when opp_id = '006Dm000003LobKIAS' then to_date('2023-06-15')
     when opp_id = '006Dm000005MfwnIAC' then to_date('2023-09-01')
     when opp_id = '006Dm0000049v2TIAQ' then to_date('2023-10-01')
     when opp_id = '006Dm000002dNKAIA2' then to_date('2023-11-01')
     when opp_id = '006Dm000005O514IAC' then to_date('2023-11-15')
     when opp_id = '006Dm000002fQa8IAE' then to_date('2023-11-29')
     ELSE start_dte_raw end as start_dte,
closed_won_dte,
date_trunc('month',to_date(start_dte)) as start_dte_month,
date_trunc('month',to_date(end_dte)) as end_dte_month,
date_trunc('month',to_date(end_dte_raw)) as end_dte_raw_month,
date_trunc('month',to_date(closed_won_dte)) as closed_won_dte_month,
CASE WHEN opp_id = '0063600000M73LuAAJ' then 200000
     WHEN opp_id = '0063600000dsPyXAAU' then 150000
     WHEN opp_id = '0061R00000kRNPDQA4' then 400000
     WHEN opp_id = '0063600000X3OBrAAN' then 480000
     WHEN opp_id = '0061R0000137ijiQAA' then 25000
     WHEN opp_id = '0063600000dsPsjAAE' then 600000
     WHEN opp_id = '0061R00000uJr07QAC' then 100000
     WHEN opp_id = '0061R0000135gO1QAI' then 89040
     when opp_id = '0061R000014xeQwQAI' then 13269
     when opp_id = '0061R00001A6F76QAF' then 15000
     when opp_id = '0061R00001BAPkAQAX' then 330000
     when opp_id = '0061R00000oERITQA4' then 520000
     when opp_id = '0061R000010tCbNQAU' then 520000
     when opp_id = '0061R00000zD2sxQAC' then 1980000
     ELSE opp_arr end as opp_arr,
CASE WHEN opp_id = '0061R0000135gO1QAI' then 5040 
     WHEN opp_id = '0061R000014xeQwQAI' then 13269
     WHEN opp_id = '0061R00001A6F76QAF' then 15000
     when opp_id = '0061R00001BAPkAQAX' then 150000
     ELSE opp_net_new_arr end as opp_net_new_arr,
opp_is_marketing_influenced_flag
from raw_data
where opp_id not in ('00636000003gG2qAAE','0063600000W0NhNAAV','0063600000SKDdAAAX','0061R00000m1g4KQAQ','0063600000X36vUAAR') /*removing these ops per FP&A */
)

select * from raw_data_transformed where opp_id not in 
(
'0061R000016my8LQAQ', /* Tokio Marine Deal 420k. Removed due to out clause */
'0061R000016jsHbQAI', /* Utilize Core 54k. Removed due to out clause */
'0061R000010QadCQAS', /* Original Tokio Marine Deal with Philly Insuarnce (replaced by amended opp for Philly Insurance) */
'0061R000014wNsNQAU', /* Data Dimensions 640k. Removing because they opted out of their auto-renewal */
'0061R00001A3TIAQA3', /* Vida Capital 1.6k. Was an NRR Deal. Should have not been in here as ARR */
'0061R000013gx5GQAQ', /* Chanel F&B 5k. Was an NRR Deal. Should have not been in here as ARR */
'0061R00000zCCLQQA4', /* Air Force opp that was mistakenly moved to closed win in SFDC */
'006Dm000003M0dVIAS', /* Paid Pilot that in not recurring. Australian Department of Defense */
'0061R00001A5wigQAB', /* Removing Peer Street because this is a churn */
'0061R00001BAugnQAD', /* Removing Pacific Life 180k Renewal because it should have never gone Closed Won */
'006Dm000005ESjnIAG', /* Removing SSA Amendment Opp because it is incorporated in the ARR adjustment to existing opp */
'0061R000019R8fwQAC', /* Removing mutual of Omaha because it was replaced by an upsell opp */
'006Dm000005ET13IAG', /* Air Force Renewal Churn Tracking */
'006Dm000005ESOCIA4', /* Mathematica Churn Tracking */
'006Dm000004AP8WIAW', /* CarMAx */
'006Pm00000FTwEWIA1' /* RTI. Can add this after 45 day trial period */
)