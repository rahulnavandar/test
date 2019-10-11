with
  chapter_usage_list as ( select a.*, Row_Number() over (Partition by a.chapterDOI order by a.chapterDOI, a.counter DESC) as Row_Num  from `{{ params.project }}.{{ params.environment }}_STG_ProductMaster.chapters_usage` as a ),
  chapter_bk_isbn as ( select distinct eISBN from chapter_usage_list),
  isbn_list as ( select distinct* from `{{ params.project }}.{{ params.environment }}_DWH_ProductMaster.Books` where BookISBN13 in ( select eISBN from chapter_bk_isbn))

select
  chapterDOI as ItemDOI,
  chapterDOI as ChapterKey,
  '' as ChapterID,
  chapterDOI as ChapterDOI,
  cast(ebook.BookIntellectualUnitID as String) as ChapterIntellectualUnitID,
  Current_Date as ChapterLastUpdateDate,
  1 as ChapterNumberofChapters,
  '' as ChapterOpenChoice,
  'N' as ChapterPhysical,
  DATE(1900, 1, 1) as ChapterPublishedOnline,
  '' as ChapterSequenceNumber,
  '' as ChapterTitle,
  eISBN as ChapterISBN,
  ebook.BookEditionID as ChapterBookEditionID,
  cast(ebook.BookIntellectualUnitID as String) as ChapterBookTitleID,
  '' as ChapterCategory,
  ebook.BookCopyrightHolder as ChapterCopyrightHolder,
  '' as ChapterCopyrightYear,
  '' as ChapterFirstPage,
  '' as ChapterLastPage,
  '' as ChapterSubCategory,
  '' as ChapterSubTitle,
  '' as ChapterRetractionReason,
  'OnlinePDF' as ChapterTargetType

  from chapter_usage_list as chapter
    left outer join isbn_list as ebook on chapter.eISBN = ebook.BookISBN13
    where Row_Num = 1 and chapterDOI != '' and chapterDOI is not NULL
 --chapterDOI = '10.1007/978-1-4020-4585-1_2688'
