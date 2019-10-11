SELECT 
  DATE_TRUNC(UsageCalenderDate,MONTH) UsageCalenderDate, 
  UsageCalenderYear, 
  UsageCalenderMonth, 
  UsageCalenderYearMonth, 
  UsageCalenderQuarter, 
  UsageAuthenticationType, 
  UsageAuthenticationValue, 
  UsagePlatform, 
  UsagePublisherName, 
  UsagePublisherType, 
  UsageBookJournalDOI, 
  UsagePublicationYear, 
  UsageItemDOI, 
  UsageContentType, 
  UsagePrintISSN, 
  UsageElectronicISSN, 
  UsagePrintISBN, 
  UsageElectronicISBN, 
  UsageSeriesISSN_E, 
  UsageSeriesISSN_P, 
  UsageReturnCode, 
  UsageAccessYN, 
  UsageFederatedSearch, 
  UsageOpenAccessYN, 
  UsageJournalNo, 
  UsageRobot, 
  UsageFullBookYN, 
  sum(UsageClicks) UsageClicks, 
  sum(UsageWholeBookDownload) UsageWholeBookDownload,
  PMKey 
FROM 
  `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Slink_Material_Base_Reporting_Daily` 
where 
  UsageCalenderyearmonth = '{{ params.yearmonthsep }}'
GROUP BY 
  DATE_TRUNC(UsageCalenderDate,MONTH), UsageCalenderYear, UsageCalenderMonth, UsageCalenderYearMonth, UsageCalenderQuarter,       UsageAuthenticationType, UsageAuthenticationValue, UsagePlatform, UsagePublisherName, UsagePublisherType, UsageBookJournalDOI,       UsagePublicationYear, UsageItemDOI, UsageContentType, UsagePrintISSN, UsageElectronicISSN, UsagePrintISBN, UsageElectronicISBN,       UsageSeriesISSN_E, UsageSeriesISSN_P, UsageReturnCode, UsageAccessYN, UsageFederatedSearch, UsageOpenAccessYN, UsageJournalNo,       UsageRobot, UsageFullBookYN, PMKey