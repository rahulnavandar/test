MERGE `{{ params.project }}.{{ params.environment }}_STG_ProductMaster.PM_UsageArticle`  T
USING 
(
SELECT 
  UsageItemDOI,
  UsageBookJournalDOI,
  UsageChapterArticleTitle,
  UsageJournalNo,
  UsagePrintISSN,
  UsageElectronicISSN,
  UsageOpenAccessYN,
  UsagePublisherName,
  UsagePublisherType,
  UsagePublicationTitle,
  UsagePublicationYear,
  Count(*) Counter
FROM
  `{{ params.project }}.{{ params.environment }}_STG_Usage.Usage_C4_{{ params.yearmonth }}` usage
WHERE
  UsageCalenderDate = '{{ params.date }}' AND
  UsagePublisherType ='ARTICLE'
GROUP BY 
  UsageItemDOI,
  UsageBookJournalDOI,
  UsageChapterArticleTitle,
  UsageJournalNo,
  UsagePrintISSN,
  UsageElectronicISSN,
  UsageOpenAccessYN,
  UsagePublisherName,
  UsagePublisherType,
  UsagePublicationTitle,
  UsagePublicationYear
  ) S
ON T.UsageItemDOI = S.UsageItemDOI
AND T.UsageBookJournalDOI = S.UsageBookJournalDOI
AND T.UsageChapterArticleTitle = S.UsageChapterArticleTitle
AND T.UsageJournalNo = S.UsageJournalNo
AND T.UsagePrintISSN = S.UsagePrintISSN
AND T.UsageElectronicISSN = S.UsageElectronicISSN
AND T.UsageOpenAccessYN = S.UsageOpenAccessYN
AND T.UsagePublisherName = S.UsagePublisherName
AND T.UsagePublisherType = S.UsagePublisherType
AND T.UsagePublicationTitle = S.UsagePublicationTitle
AND T.UsagePublicationYear = S.UsagePublicationYear
WHEN NOT MATCHED THEN
  INSERT(UsageItemDOI,
  UsageBookJournalDOI,
  UsageChapterArticleTitle,
  UsageJournalNo,
  UsagePrintISSN,
  UsageElectronicISSN,
  UsageOpenAccessYN,
  UsagePublisherName,
  UsagePublisherType,
  UsagePublicationTitle,
  UsagePublicationYear,
  Counter,
  UpdatedOn)
  VALUES(S.UsageItemDOI,
  S.UsageBookJournalDOI,
  S.UsageChapterArticleTitle,
  S.UsageJournalNo,
  S.UsagePrintISSN,
  S.UsageElectronicISSN,
  S.UsageOpenAccessYN,
  S.UsagePublisherName,
  S.UsagePublisherType,
  S.UsagePublicationTitle,
  S.UsagePublicationYear,
  S.Counter,
  CURRENT_DATETIME())
WHEN MATCHED THEN
  UPDATE SET 
  Counter =  T.Counter + S.Counter,
  UpdatedOn = CURRENT_DATETIME()