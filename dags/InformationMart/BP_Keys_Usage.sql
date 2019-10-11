CREATE OR REPLACE TABLE  `{{ params.project }}.{{ params.environment }}_DWH_BusinessPartner.BusinessPartner_SAPBW_BK_Usage`
AS 
SELECT DISTINCT UsageBusinessPartner  FROM `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_BP_Base_Reporting_MonthlyV2`
UNION DISTINCT 
select distinct u.UsageBusinessPartner from
`{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Magnus_BP_Base_Reporting_MonthlyV2` u
UNION DISTINCT
select distinct u.UsageBusinessPartner from
`{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Nature_BP_Base_Reporting_MonthlyV2` u 
