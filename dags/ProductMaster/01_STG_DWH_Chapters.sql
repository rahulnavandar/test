WITH
  uniquebk AS (
  SELECT DISTINCT
    upper(ItemDOI) ItemDOI
  FROM
     `{{ params.project }}.{{ params.environment }}_STG_ProductMaster.BK_ChapterDOI`
  ),
  getcolumns AS (
  SELECT
    upper(ItemDOI) ItemDOI,
    upper(Coalesce(dds_diff.CHAPTERDOI,
      dds_full.CHAPTERDOI))  ChapterKey,
    Coalesce(dds_diff.CHAPTERID,
      dds_full.CHAPTERID) ChapterID,
    upper(Coalesce(dds_diff.CHAPTERDOI,
      dds_full.CHAPTERDOI)) ChapterDOI,
    Coalesce(dds_diff.BOOKTITLEID,
      dds_full.BOOKTITLEID) ChapterIntellectualUnitID,
    Coalesce(dds_diff.LASTUPLOADDATE,
      dds_full.LASTUPLOADDATE) ChapterLastUpdateDate,
    1 ChapterNumberofChapters,
    Coalesce(dds_diff.OPENCHOICE,
      dds_full.OPENCHOICE) ChapterOpenChoice,
    'Y' ChapterPhysical,
    PARSE_DATE('%Y%m%d',
      Coalesce(dds_diff.CHAPTERONLINEDATE,
          dds_full.CHAPTERONLINEDATE)) ChapterPublishedOnline,
--          replace(cast(book_parts.Chapter_online_date as string), "-", "")))) ChapterPublishedOnline,
    Coalesce(dds_diff.CHAPTERSEQUENCENUMBER,
      dds_full.CHAPTERSEQUENCENUMBER) ChapterSequenceNumber,
--      cast(book_parts.Chapter_sequence_number as string)) ChapterSequenceNumber,
    Coalesce(dds_diff.CHAPTERTITLE,
      dds_full.CHAPTERTITLE) ChapterTitle,
    Coalesce(dds_diff.ISBN,
      dds_full.ISBN,
      usage_md.UsagePrintISBN,
	  usage_md.UsageElectronicISBN) ChapterISBN,
    Coalesce(dds_diff.BOOKEDITIONID,
      dds_full.BOOKEDITIONID) ChapterBookEditionID,
    Coalesce(dds_diff.BOOKTITLEID,
      dds_full.BOOKTITLEID) ChapterBookTitleID,
    Coalesce(dds_diff.CHAPTERCATEGORY,
      dds_full.CHAPTERCATEGORY) ChapterCategory,
    Coalesce(dds_diff.CHAPTERCOPYRIGHTHOLDER,
      dds_full.CHAPTERCOPYRIGHTHOLDER) ChapterCopyrightHolder,
    Coalesce(dds_diff.CHAPTERCOPYRIGHTYEAR,
      dds_full.CHAPTERCOPYRIGHTYEAR) ChapterCopyrightYear,
--      cast(book_parts.Chapter_copyright_year as string)) ChapterCopyrightYear,
    Coalesce(dds_diff.CHAPTERFIRSTPAGE,
      dds_full.CHAPTERFIRSTPAGE) ChapterFirstPage,
--      cast(book_parts.Chapter_first_page as string)) ChapterFirstPage,
    Coalesce(dds_diff.CHAPTERLASTPAGE,
      dds_full.CHAPTERLASTPAGE) ChapterLastPage,
--      cast(book_parts.Chapter_last_page as string)) ChapterLastPage,
    Coalesce(dds_diff.CHAPTERSUBCATEGORY,
      dds_full.CHAPTERSUBCATEGORY) ChapterSubCategory,
    Coalesce(dds_diff.CHAPTERSUBTITLE,
      dds_full.CHAPTERSUBTITLE) ChapterSubTitle,
    Coalesce(dds_diff.CHAPTERRETRACTIONREASON,
      dds_full.CHAPTERRETRACTIONREASON) ChapterRetractionReason,
    Coalesce(dds_diff.CHAPTERTARGETTYPE,
      dds_full.CHAPTERTARGETTYPE) ChapterTargetType
  FROM
    uniquebk bk
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.dds_chapters_diff` dds_diff
  ON
    upper(bk.ItemDOI) = upper(dds_diff.CHAPTERDOI)
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.dds_chapters_full` dds_full
  ON
    upper(bk.ItemDOI) = upper(dds_full.CHAPTERDOI)
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_STG_ProductMaster.VW_PM_UsageChapterBook` usage_md
  ON
    upper(bk.ItemDOI) = upper(usage_md.UsageItemDOI))
--  LEFT JOIN
--    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.dds_chapters_book_parts` book_parts
--  ON
--    upper(bk.ItemDOI) = upper(book_parts.Chapter_DOI))
SELECT
  *
FROM
  getcolumns