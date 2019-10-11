SELECT 
  'Dummy_FromDatabasePlatform' as pmkey,
  DATE_TRUNC(UsageCalenderDate, MONTH) UsageCalenderDate, 
  UsageCalenderYear, 
  UsageCalenderMonth, 
  UsageCalenderYearMonth, 
  UsageCalenderQuarter, 
  UsageBusinessPartner, 
  UsagePlatform, 
  UsageContentType, 
  UsageAccessYN, 
  UsageFederatedSearch, 
  UsageDatabase, 
  UsageDatabaseId,
  sum(UsageClicks) UsageClicks 
FROM 
  `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Database_BP_Base_Reporting_Daily`
where UsageCalenderyearmonth = '{{ params.yearmonthsep }}' 
GROUP BY 
  DATE_TRUNC(UsageCalenderDate, MONTH), UsageCalenderYear, UsageCalenderMonth, UsageCalenderYearMonth, UsageCalenderQuarter,   UsageBusinessPartner, UsagePlatform, UsageContentType, UsageAccessYN, UsageFederatedSearch, UsageDatabase, UsageDatabaseId
