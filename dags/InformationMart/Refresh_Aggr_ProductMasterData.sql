CREATE OR REPLACE TABLE `{{ params.project }}.{{ params.environment }}_DWH_ProductMaster.UsageProductMaster_BP`
AS
SELECT
usage.PMKey,
  usage.UsageItemDOI,
  CASE UsagePublisherType WHEN 'ARTICLE' THEN j.JournalDOI ELSE  b.BookDOI END As DOI,
  --Attributes related to Articles/Journals or Chapters/Books
  CASE UsagePublisherType WHEN 'ARTICLE' THEN a.ArticleTitle ELSE  c.ChapterTitle END As ItemDOI_Title,
  CASE UsagePublisherType WHEN 'ARTICLE' THEN j.JournalImprint ELSE  b.BookImprint END As Imprint,
  CASE UsagePublisherType WHEN 'ARTICLE' THEN timj.TERM ELSE  timb.TERM END As ImprintDesc,
  CASE UsagePublisherType WHEN 'ARTICLE' THEN j.JournalPublisher ELSE  b.BookPublisher END As Publisher,
  CASE UsagePublisherType WHEN 'ARTICLE' THEN tpuj.PUBLISHER_NAME ELSE  tpub.PUBLISHER_NAME END As PublisherDesc,
  CASE UsagePublisherType WHEN 'ARTICLE' THEN  j.JournalPDDesc ELSE   b.BookPDDesc END As PD,
  CASE UsagePublisherType WHEN 'ARTICLE' THEN  j.JournalPSDesc ELSE   b.BookPSDesc END As PS,
  CASE UsagePublisherType WHEN 'ARTICLE' THEN  j.JournalPUDesc ELSE   b.BookPUDesc END As PU,
  CASE UsagePublisherType WHEN 'ARTICLE' THEN  j.JournalMainDiscipline ELSE   b.BookMainDiscipline END As Discipline,
  CASE UsagePublisherType WHEN 'ARTICLE' THEN  tmdj.TERM ELSE  tmdb.TERM END As DisciplineDesc,
  CASE UsagePublisherType WHEN 'ARTICLE' THEN  a.ArticleAuthor ELSE   b.BookAuthors END As Author,
  --Article/Journal attributes
  usage.UsageJournalNo,
  j.JournalTitle As Journal,
  j.JournalISSNElectronic As Online_ISSN,
  j.JournalISSNPrint As Print_ISSN,
  j.JournalSubjectCollectionDescription As Subject_Collection,
  j.JournalOpenAccessType,
  a.ArticleVolume,
  a.ArticleIssue,
  a.ArticlePricelistYear,
  a.ArticlePublishedOnlineDateJWF,
  Coalesce((safe_cast(a.ArticleAnnual as INT64)),(EXTRACT(YEAR FROM a.ArticlePublishedOnlineDateDDS)), (EXTRACT(YEAR FROM a.ArticlePublishedOnlineDateJWF))) as ArticlePubYear,
  --Coalesce((EXTRACT(YEAR FROM a.ArticlePublishedOnlineDateDDS)), (EXTRACT(YEAR FROM a.ArticlePublishedOnlineDateJWF)), (safe_cast(a.ArticleAnnual as INT64))) as ArticlePubYear,
  j.JournalReportingKey,
  j.JournalPrice_EUR, -- Added 5/13
  j.JournalPrice_USD, --Added 5/13
  --Chapter/Book attributes
  b.BookEbookPackage,
  b.BookEbookPackageDesc,
  b.BookEditionYear,
  b.BookCategory,
  pc.Term BookCategoryDesc,
  b.BookSeriesNumber,
  b.BookSeriesTitle,
  b.BookSubjectCollectionDescr,
  b.BookSpringerProjects,
  tsp.TERM BookSpringerProjectsDesc,
  b.BookISBNEbook As eBook_ISBN,
  b.BookISBN13 As Print_ISBN,
  b.BookTitle,
  b.BookMainLanguage,
  b.BookPriceUSD,
  b.BookPriceEUR
  FROM
`{{ params.project }}.{{ params.environment }}_STG_Usage.Test_PM_Aggr` usage
LEFT JOIN `{{ params.project }}.{{ params.environment }}_DWH_ProductMaster.Journals` j ON usage.UsageJournalNo = j.JournalTitleNo  --Join Journals table
LEFT JOIN `{{ params.project }}.{{ params.environment }}_DWH_ProductMaster.Articles` a on usage.UsageItemDOI = a.DOI  -- Join Articles table
LEFT JOIN `{{ params.project }}.{{ params.environment }}_DWH_ProductMaster.Chapters` c on usage.UsageItemDOI = c.ItemDOI --Join Chapters table
LEFT JOIN `{{ params.project }}.{{ params.environment }}_DWH_ProductMaster.Books` b on c.ChapterISBN  = b.BookISBN13  --Join Books table
-- LEFT JOIN `usage-data-reporting.DEV_DWH_ProductMaster.Books` b on usage.UsageBookJournalDOI = b.BookDOI  --Join Books table
LEFT JOIN `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_springer_projects` tsp on b.BookSpringerProjects = tsp.CODE
LEFT JOIN (SELECT DISTINCT PUBLISHER_CODE , FIRST_VALUE( PUBLISHER_NAME ) OVER (PARTITION BY PUBLISHER_CODE ORDER BY PUBLISHER_NAME ASC) PUBLISHER_NAME FROM `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_publisher` ) tpuj on j.JournalPublisher = tpuj.PUBLISHER_CODE
LEFT JOIN (SELECT DISTINCT PUBLISHER_CODE , FIRST_VALUE( PUBLISHER_NAME ) OVER (PARTITION BY PUBLISHER_CODE ORDER BY PUBLISHER_NAME ASC) PUBLISHER_NAME FROM `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_publisher` ) tpub on b.BookPublisher = tpub.PUBLISHER_CODE
LEFT JOIN  `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_main_discipline` tmdj on j.JournalMainDiscipline = tmdj.CODE
LEFT JOIN  `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_main_discipline` tmdb on b.BookMainDiscipline  = tmdb.CODE
LEFT JOIN (SELECT DISTINCT CODE, FIRST_VALUE(TERM) OVER (PARTITION BY CODE ORDER BY TERM ASC) TERM FROM `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_imprint` ) timj on j.JournalImprint = timj.CODE
LEFT JOIN (SELECT DISTINCT CODE, FIRST_VALUE(TERM) OVER (PARTITION BY CODE ORDER BY TERM ASC) TERM FROM `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_imprint` ) timb on b.BookImprint = timb.CODE
LEFT JOIN (SELECT distinct substr(CODE, 4,99) CODE, TERM FROM `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_product_category`) pc on b.BookCategory = pc.CODE