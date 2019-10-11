SELECT 
  PMKey,
  DATE_TRUNC(UsageCalenderDate, MONTH) UsageCalenderDate, 
  UsageCalenderYear, 
  UsageCalenderMonth, 
  UsageCalenderYearMonth, 
  UsageCalenderQuarter, 
  UsageBusinessPartner, 
  UsageBusinessPartnerType, 
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
  LTRIM(UsageJournalNo, '0') as UsageJournalNo, 
  UsageFullBookYN, 
  Usagesiteid,
  sum(UsageClicks) UsageClicks 
FROM 
  `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Nature_BP_Base_Reporting_Daily` 
where 
  UsageCalenderyearmonth = '{{ params.yearmonthsep }}' 
GROUP BY 
  PMKey,DATE_TRUNC(UsageCalenderDate, MONTH), UsageCalenderYear, UsageCalenderMonth, UsageCalenderYearMonth, UsageCalenderQuarter,     UsageBusinessPartner, UsageBusinessPartnerType, UsageAuthenticationType, UsageAuthenticationValue, UsagePlatform, UsagePublisherName,     UsagePublisherType, UsageBookJournalDOI, UsagePublicationYear, UsageItemDOI, UsageContentType, UsagePrintISSN, UsageElectronicISSN,     UsagePrintISBN, UsageElectronicISBN, UsageSeriesISSN_E, UsageSeriesISSN_P, UsageReturnCode, UsageAccessYN, UsageFederatedSearch,     UsageOpenAccessYN, UsageJournalNo, UsageFullBookYN, Usagesiteid