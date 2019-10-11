with
  article_usage_list as ( select a.*, Row_Number() over (Partition by a.articleDOI order by a.articleDOI, a.counter DESC) as Row_Num  from `{{ params.project }}.{{ params.environment }}_STG_ProductMaster.articles_usage` as a ),
  article_issn_list as (select distinct eISSN from article_usage_list),
  issn_list as (select distinct * from `{{ params.project }}.{{ params.environment }}_DWH_ProductMaster.Journals` where JournalISSNElectronic in (select eISSN from article_issn_list) or JournalISSNPrint in (select eISSN from article_issn_list))

select
 articleDOI as DOI,
 '' as ArticleNbrofColoredPages,
 0 as ArticleNbrofFreeEoffPrints,
 0 as ArticleNbrFreeoffprints,
 0 as ArticleNbrofPaidEoffPrints,
 0 as ArticleNbrofPaidOffprints,
 0 as POSTERS,
 DATE(1900, 1, 1) as ArticleAccepted,
 '' as ArticleCategory,
 '' as ArticleID,
 DATE(1900, 1, 1) as ArticleModificationDate,
 DATE(1900, 1, 1) as ArticleRegistrationDate,
 '' as ArticleType,
 '' as ArticleAuthor,
 '' as ArticleBodyMarkup,
 DATE(1900, 1, 1) as ArticleCheckPCQuality,
 DATE(1900, 1, 1) as ArticleCheckProofQuality,
 '' as ArticleCopyEditingCategory,
 CURRENT_DATE as ArticleCreationDate,
 '' as ArticleCurrentStatus,
 articleDOI as ArticleDOI,
 concat('https://dx.doi.org/', articleDOI) as ArticleDOILink,
 DATE(1900, 1, 1) as ArticleFirstCheckInDate,
 '' as ArticleFirstPage,
 DATE(1900, 1, 1) as ArticleInIssueWorkflow,
 DATE(1900, 1, 1) as ArticleInPrepareContent,
 '' as ArticleInternetMailAddress,
 safe_cast(coalesce( eJournal.JournalKey, pJournal.JournalKey, articles.JournalTitle) as Int64) as ArticleJournalTitleNumber,
 DATE(1900, 1, 1) as ArticleLastCheckInDateDDS,
 '' as ArticleLastpage,
 '' as ArticleManuscriptNumber,
 '' as ArticleNoteforProdEditor,
 '' as ArticleNoteonIssueBuilding,
 '' as ArticleObjectName,
 '' as ArticleOpenAccess,
 DATE(1900, 1, 1) as ArticlePCQualityDisapproved,
 DATE(1900, 1, 1) as ArticlePerformProofCorrections,
 DATE(1900, 1, 1) as ArticlePrepareContentComplete,
 DATE(1900, 1, 1) as ArticlePrepareforPublication,
 '' as ArticlePricelistYear,
 '' as ArticlePrinter,
 DATE(1900, 1, 1) as ArticleProductionDeadline,
 DATE(1900, 1, 1) as ArticleProductionWorkflowStarted,
 DATE(1900, 1, 1) as ArticleProofQualityApproved,
 DATE(1900, 1, 1) as ArticleProofQualityDisapproved,
 DATE(1900, 1, 1) as ArticlePublishedOnlineDateDDS,
 DATE(1900, 1, 1) as ArticlePublishedOnlineDateJWF,
 DATE(1900, 1, 1) as ArticleReceivedbyDDS,
 DATE(1900, 1, 1) as ArticleReleaseDate,
 '' as ArticleS150State,
 DATE(1900, 1, 1) as ArticleSendingProof,
 DATE(1900, 1, 1) as ArticleSentforPrepareContent,
 DATE(1900, 1, 1) as ArticleSenttoDDS,
 0 as ArticleSequenceNumber,
 '' as ArticleTitle,
 '' as ArticleVendor,
 '' as ArticleVolume,
 safe_cast(coalesce( eJournal.JournalKey, pJournal.JournalKey, articles.JournalTitle) as Int64) as ArticleJournal,
 '' as ArticleAnnual,
 '' as ArticleIssue,
 '' as ArticleIssueKey,
 '' as ArticleOCOrgname,
 CURRENT_DATE as ArticleLastUploadDate,
 '' as ArticleIssueType,
 0 as ArticlePublishedOFdays
 from article_usage_list as articles
   left outer join issn_list as eJournal on articles.eISSN = eJournal.JournalISSNElectronic
   left outer join issn_list as pJournal on articles.eISSN = pJournal.JournalISSNPrint
    where Row_Num = 1 and articleDOI != '' and articleDOI is not NULL