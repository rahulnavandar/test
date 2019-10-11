CREATE OR REPLACE TABLE `{{ params.project }}.{{ params.environment }}_DWH_BusinessPartner.BusinessPartner_SAPBW_Usage` 
AS 
SELECT * FROM `{{ params.project }}.{{ params.environment }}_DWH_BusinessPartner.BusinessPartner_SAPBW` BP
WHERE BP.BusinessPartnerID IN (
SELECT UsageBusinessPartner  FROM `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_BP_Base_Reporting_MonthlyV2`
)