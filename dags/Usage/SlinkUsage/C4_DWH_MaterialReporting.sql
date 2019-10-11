WITH
  add_rownumber AS ( 
  SELECT
    *, 0 as UsageWholeBookDownload,
    ROW_NUMBER() OVER (PARTITION BY UsageCalenderDate, UsageTime, UsageMillisec,UsageSessionID, UsageItemDOI ORDER BY UsageItemDOI) row_number
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_Usage.Usage_C4_{{ params.yearmonth }}`
      where UsageCalenderDate = '{{ params.date }}')
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
  UsageBPName,
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
  UsageRobot,
  UsageFullBookYN,
  1 UsageClicks,
  UsageWholeBookDownload,
  current_datetime() LoadedOn,
  usageitemdoi as PMKey
FROM
  add_rownumber where row_number = 1