WITH
  RemoveDuplicates AS (
  SELECT
    DISTINCT UsageCalenderDate,
    UsageCalenderDateTime,
    UsageCalenderDateFile,
    UsageMillisec,
    UsageCalenderYear,
    UsageCalenderMonth,
    UsageCalenderYearMonth,
    UsageCalenderQuarter,
    UsageSessionID,
    UsageTime,
    UsageTimeFull,
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
    UsageFullBookYN,
    1 UsageClicks
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_Usage.Usage_C4_{{ yesterday_ds_nodash }}`),
  AddRowNumber AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY UsageCalenderDate, UsageTime, UsageSessionID ORDER BY UsageItemDOI) FullBookClickFlag
  FROM
    RemoveDuplicates),
  SetWholeBookDownload AS (
  SELECT
    *,
    CASE
      WHEN usagefullbookyn = 'Y' AND FullBookClickFlag = 1 THEN 1
      ELSE 0
    END WholeBookDownload
  FROM
    AddRowNumber)
SELECT
  DISTINCT UsageCalenderDate,
  UsageCalenderDateTime,
  UsageCalenderDateFile,
  UsageMillisec,
  UsageCalenderYear,
  UsageCalenderMonth,
  UsageCalenderYearMonth,
  UsageCalenderQuarter,
  UsageTime,
  UsageTimeFull,
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
  UsageFullBookYN,
  1 UsageClicks,
  WholeBookDownload UsageWholeBookDownload,
  current_datetime() LoadedOn
FROM
  SetWholeBookDownload