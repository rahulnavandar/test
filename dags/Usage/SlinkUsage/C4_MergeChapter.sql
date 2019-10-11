MERGE `{{ params.project }}.{{ params.environment }}_STG_ProductMaster.PM_UsageChapterBook`  T
USING 
(
SELECT
  UsageItemDOI,
  UsageBookJournalDOI,
  UsageChapterArticleTitle,
  UsageOpenAccessYN,
  UsageSeriesISSN_E,
  UsageSeriesISSN_P,
  UsagePublisherName,
  UsagePublisherType,
  UsagePublicationTitle,
  UsagePublicationYear,
  UsagePrintISBN,
  UsageElectronicISBN,
  Count(*) Counter
FROM
  `{{ params.project }}.{{ params.environment }}_STG_Usage.Usage_C4_{{ params.yearmonth }}` usage
WHERE
  UsageCalenderDate = '{{ params.date }}' AND
  UsagePublisherType IN ('CHAPTER','REFERENCEWORKENTRY','PROTOCOL') 
GROUP BY 
  UsageItemDOI,
  UsageBookJournalDOI,
  UsageChapterArticleTitle,
  UsageOpenAccessYN,
  UsageSeriesISSN_E,
  UsageSeriesISSN_P,
  UsagePublisherName,
  UsagePublisherType,
  UsagePublicationTitle,
  UsagePublicationYear,
  UsagePrintISBN,
  UsageElectronicISBN
) S
ON T.UsageItemDOI = S.UsageItemDOI
AND T.UsageBookJournalDOI = S.UsageBookJournalDOI
AND T.UsageChapterArticleTitle = S.UsageChapterArticleTitle
AND T.UsageOpenAccessYN = S.UsageOpenAccessYN
AND T.UsageSeriesISSN_E = S.UsageSeriesISSN_E
AND T.UsageSeriesISSN_P = S.UsageSeriesISSN_P
AND T.UsagePublisherName = S.UsagePublisherName
AND T.UsagePublisherType = S.UsagePublisherType
AND T.UsagePublicationTitle = S.UsagePublicationTitle
AND T.UsagePublicationYear = S.UsagePublicationYear
AND T.UsagePrintISBN = S.UsagePrintISBN
AND T.UsageElectronicISBN = S.UsageElectronicISBN
WHEN NOT MATCHED THEN
  INSERT(UsageItemDOI,
  UsageBookJournalDOI,
  UsageChapterArticleTitle,
    UsageOpenAccessYN,
    UsageSeriesISSN_E,
  UsageSeriesISSN_P,
  UsagePublisherName,
  UsagePublisherType,
  UsagePublicationTitle,
  UsagePublicationYear,
  UsagePrintISBN,
  UsageElectronicISBN,
  Counter,
  UpdatedOn)
  VALUES(S.UsageItemDOI,
  S.UsageBookJournalDOI,
  S.UsageChapterArticleTitle,
    S.UsageOpenAccessYN,
    UsageSeriesISSN_E,
  UsageSeriesISSN_P,
  S.UsagePublisherName,
  S.UsagePublisherType,
  S.UsagePublicationTitle,
  S.UsagePublicationYear,
  S.UsagePrintISBN,
  S.UsageElectronicISBN,
  S.Counter,
  CURRENT_DATETIME())
WHEN MATCHED THEN
  UPDATE SET 
  Counter = T.Counter + S.Counter,
  UpdatedOn = CURRENT_DATETIME()