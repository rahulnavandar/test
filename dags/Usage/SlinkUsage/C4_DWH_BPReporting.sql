SELECT
  UsageCalenderDate,
  UsageCalenderDateTime,
  UsageCalenderDateFile,
  UsageMillisec,
  UsageCalenderYear,
  UsageCalenderMonth,
  UsageCalenderYearMonth,
  UsageCalenderQuarter,
  UsageTime,
  UsageTimeFull,
  UsageSessionID,
  UsageClientIP,
  UsageClientIPSearch,
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
  UsageClicks,
  CURRENT_DATETIME LoadedOn,
  usageitemdoi as PMKey
FROM
  `{{ params.project }}.{{ params.environment }}_STG_Usage.Usage_C4_{{ params.yearmonth }}`
where UsageCalenderDate = '{{ params.date }}'