WITH
  bk_books AS (
  SELECT
    DISTINCT BookKey
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_ProductMaster.BK_BookDOI`),
book_prices as (
SELECT ORDER_NUMBER, MAX(PRICE_EUR) PRICE_EUR, MAX(PRICE_USD) PRICE_USD FROM `usage-data-reporting.DEV_LDZ_ProductMaster.bflux_book_prices` 
GROUP BY ORDER_NUMBER
),
  getcolumns AS (
  SELECT
    BookKey,
    CAST(Coalesce(books_diff.NR_COLOR_PAGES,
        books_full.NR_COLOR_PAGES) AS INT64) BookNbrColoredPages,
    CAST(Coalesce(books_diff.ILLUSTRATIONS_BW,
        books_full.ILLUSTRATIONS_BW) AS INT64) BookNbrIllustrationBW,
    CAST(Coalesce(books_diff.ILLUSTRATIONS_C,
        books_full.ILLUSTRATIONS_C) AS INT64) BookNbrIllustrationColor,
    Coalesce(books_diff.AUTHOR,
      books_full.AUTHOR) BookAuthors,
    Coalesce(books_diff.AVAIL_TO_SAMPLE,
      books_full.AVAIL_TO_SAMPLE) BookAvailabletoSample,
    Coalesce(books_diff.BIBLIOGRAPH_INFO,
      books_full.BIBLIOGRAPH_INFO) BookBibliographicInformaton,
    Coalesce(books_diff.BINDER,
      books_full.BINDER) BookBinder,
    UPPER(Coalesce(books_diff.BINDING_TYPE,
        books_full.BINDING_TYPE)) BookBindingType,
    Coalesce(books_diff.BODY_MARKUP,
      books_full.BODY_MARKUP) BookBodyMarkup,
    CASE
      WHEN Coalesce(books_diff.BULK_ONLY,  books_full.BULK_ONLY) = 0 THEN "N"
      WHEN Coalesce(books_diff.BULK_ONLY,
      books_full.BULK_ONLY) = 1 THEN "Y"
      ELSE NULL
    END BookBulkOnly,
    Coalesce(books_diff.CALC_PRICE,
      books_full.CALC_PRICE) BookCalculatedPrice,
    Coalesce(books_diff.COMPANY_CODE,
      books_full.COMPANY_CODE) BookCompanyCode,
    CASE
      WHEN Coalesce(books_diff.COLOR_IMAGES_PR,  books_full.COLOR_IMAGES_PR) = 0 THEN "N"
      WHEN Coalesce(books_diff.COLOR_IMAGES_PR,
      books_full.COLOR_IMAGES_PR) = 1 THEN "Y"
      ELSE NULL
    END BookContainsColorImages,
    Coalesce(books_diff.COPYEDITING_CAT,
      books_full.COPYEDITING_CAT) BookCopyEditingCategory,
    Coalesce(books_diff.COPYRIGHT_HOLDER,
      books_full.COPYRIGHT_HOLDER) BookCopyrightHolder,
    Coalesce(books_diff.CRY_FREEZE,
      books_full.CRY_FREEZE) BookCopyrightFreeze,
    Coalesce(books_diff.COVER_DESIGN,
      books_full.COVER_DESIGN) BookCoverDesigner,
    Coalesce(books_diff.COVER_PRINTER,
      books_full.COVER_PRINTER) BookCoverPrinter,
    Coalesce(books_diff.COVER_PRD_INF_FN,
      books_full.COVER_PRD_INF_FN) BookCoverProdInfoFinishing,
    Coalesce(books_diff.COVER_TYPE,
      books_full.COVER_TYPE) BookCoverType,
    PARSE_DATE('%Y%m%d',
      Coalesce(books_diff.REND_CREAT_DAT,
        books_full.REND_CREAT_DAT)) BookCreationDateRendition,
    UPPER(Coalesce(books_diff.PRODUCTION_STATE,
        books_full.PRODUCTION_STATE)) BookCurrentStatus,
    Coalesce(books_diff.DESK_EDITOR,
      books_full.DESK_EDITOR) BookDeskEditor,
    Coalesce(books_diff.DISCOUNT_GROUP,
      books_full.DISCOUNT_GROUP) BookDiscountGroup,
    Coalesce(books_diff.RESPONS_PERSON,
      books_full.RESPONS_PERSON) BookDistirbutionDealEmergingMarket,
    UPPER(Coalesce(books_diff.DOI,
        books_full.DOI)) BookDOI,
    Coalesce(books_diff.EAN,
      books_full.EAN) BookEANCode,
    Coalesce(substr(books_diff.EBOOK_PACKAGE,5,5),
      substr(books_full.EBOOK_PACKAGE,5,5)) BookEbookPackage,
    Coalesce(books_diff.EDITION_ID,
      books_full.EDITION_ID) BookEditionID,
    UPPER(Coalesce(books_diff.EDITION_NAME,
        books_full.EDITION_NAME)) BookEditionName,
    Coalesce(books_diff.EDITION_NO,
      books_full.EDITION_NO) BookEditionNo,
    Coalesce(books_diff.COPYRIGHT_YEAR,
      books_full.COPYRIGHT_YEAR) BookEditionYear,
    Coalesce(books_diff.RESPONS_PERSON,
      books_full.RESPONS_PERSON) BookEntity,
    CASE
      WHEN Coalesce(books_diff.E_ONLY_EDITION,  books_full.E_ONLY_EDITION) = "0" THEN "N"
      WHEN Coalesce(books_diff.E_ONLY_EDITION,
      books_full.E_ONLY_EDITION) = "1" THEN "Y"
      ELSE NULL
    END BookEOnlyEdition,
    UPPER(Coalesce(books_diff.FORMAT_TRIMSIZE,
        books_full.FORMAT_TRIMSIZE)) BookFormatTrimsize,
    PARSE_DATE('%Y%m%d',
      Coalesce(books_diff.LOG_PROMOCHECK_COMPLETED,
        books_full.LOG_PROMOCHECK_COMPLETED)) BookFullPromotionCheckCompleted,
    Coalesce(books_diff.FSV,
      books_full.FSV) BookFullServiceVendor,
    Coalesce(books_diff.RESPONS_PERSON,
      books_full.RESPONS_PERSON) BookGlobalPublishingDiscipline,
    Coalesce(books_diff.RESPONS_PERSON,
      books_full.RESPONS_PERSON) BookGlobalPublishingSegment,
    Coalesce(books_diff.RESPONS_PERSON,
      books_full.RESPONS_PERSON) BookGlobalPublishingUnit,
    Coalesce(books_diff.COMPANY_GROUP,
      books_full.COMPANY_GROUP) BookGroupofCompany,
    PARSE_DATE('%Y%m%d',
      Coalesce(books_diff.LOG_HOTP,
        books_full.LOG_HOTP)) BookHandedovertoProduction,
    Coalesce(books_diff.HANDOVER_MS_REV,
      books_full.HANDOVER_MS_REV) BookHandoverMSReview,
    CASE
      WHEN Coalesce(books_diff.HIGH_QUALITY_PTO,  books_full.HIGH_QUALITY_PTO) = 0 THEN "N"
      WHEN Coalesce(books_diff.HIGH_QUALITY_PTO,
      books_full.HIGH_QUALITY_PTO) = 1 THEN "Y"
      ELSE NULL
    END BookHighQualityPTO,
    CASE
      WHEN Coalesce(books_diff.HYBRID_BOOK,  books_full.HYBRID_BOOK) = 0 THEN "N"
      WHEN Coalesce(books_diff.HYBRID_BOOK,
      books_full.HYBRID_BOOK) = 1 THEN "Y"
      ELSE NULL
    END BookHybridBook,
    Coalesce(books_diff.IMPRINT,
      books_full.IMPRINT) BookImprint,
    Coalesce(books_diff.BOOK_TITLE_ID,
      books_full.BOOK_TITLE_ID) BookIntellectualUnitID,
    Coalesce(books_diff.ISBN,
      books_full.ISBN) BookISBN13,
    PARSE_DATE('%Y%m%d',
      Coalesce(books_diff.LOG_ISBN_ASSIGNED,
        books_full.LOG_ISBN_ASSIGNED)) BookISBNAssigned,
    Coalesce(books_diff.ISBN_EBOOK,
      books_full.ISBN_EBOOK) BookISBNEbook,
    Coalesce(books_diff.ISBN_MAINPRINT,
      books_full.ISBN_MAINPRINT) BookISBNMainprint,
    Coalesce(books_diff.ISBN_MYCOPY,
      books_full.ISBN_MYCOPY) BookISBNMyCopy,
    PARSE_DATE('%Y%m%d',
      Coalesce(books_diff.LAST_ACTION_DATE,
        books_full.LAST_ACTION_DATE)) BookLastActionDate,
    Coalesce(books_diff.MAIN_DISCIPLINE,
      books_full.MAIN_DISCIPLINE) BookMainDiscipline,
    Coalesce(books_diff.LANGUAGE,
      books_full.LANGUAGE) BookMainLanguage,
    Coalesce(books_diff.MAIN_LOCATION,
      books_full.MAIN_LOCATION) BookLocationProductionEditor,
    PARSE_DATE('%Y%m%d',
      Coalesce(books_diff.MAN_ACT_DEL_DATE,
        books_full.MAN_ACT_DEL_DATE)) BookManuscriptActualDeliveryDate,
    PARSE_DATE('%Y%m%d',
      Coalesce(books_diff.MAN_EST_DEL_DATE,
        books_full.MAN_EST_DEL_DATE)) BookManuscriptEstimatedDeliveryDate,
    Coalesce(books_diff.MARK_CLASS_DACH,
      books_full.MARK_CLASS_DACH) BookMarketingClassificationDACH,
    Coalesce(books_diff.MARK_CLASS_ROW,
      books_full.MARK_CLASS_ROW) BookMarketingClassificationROW,
    Coalesce(books_diff.MARK_CLASS_US,
      books_full.MARK_CLASS_US) BookMarketingClassificationUS,
    Coalesce(books_diff.RENDITION_TYPE,
      books_full.RENDITION_TYPE) BookMedium,
    Coalesce(books_diff.MS_APPROVED,
      books_full.MS_APPROVED) BookMSApproved,
    Coalesce(books_diff.MS_REJECTED,
      books_full.MS_REJECTED) BookMSRejected,
    CASE
      WHEN Coalesce(books_diff.NO_E_RIGHTS,  books_full.NO_E_RIGHTS) = 0 THEN "N"
      WHEN Coalesce(books_diff.NO_E_RIGHTS,
      books_full.NO_E_RIGHTS) = 1 THEN "Y"
      ELSE NULL
    END BookNoErights,
    Coalesce(books_diff.NR_COLOR_PAGES,
      books_full.NR_COLOR_PAGES) BookNbrofColoredPages,
    Coalesce(books_diff.ILLUSTRATIONS_BW,
      books_full.ILLUSTRATIONS_BW) BookNbrofIllustrationBW,
    Coalesce(books_diff.ILLUSTRATIONS_C,
      books_full.ILLUSTRATIONS_C) BookNbrofIllustrationsColor,
    Coalesce(books_diff.PAGES_MANUS,
      books_full.PAGES_MANUS) BookNbrofManuscriptPages,
    Coalesce(books_diff.PAGES_ARABIC,
      books_full.PAGES_ARABIC) BookNbrofPagesArabic,
    Coalesce(books_diff.PAGES_ROMAN,
      books_full.PAGES_ROMAN) BookNbrofPagesRoman,
    Coalesce(books_diff.TABLES_BW,
      books_full.TABLES_BW) BookNbrofTablesBW,
    Coalesce(books_diff.TABLES_C,
      books_full.TABLES_C) BookNbrofTablesColor,
    CASE
      WHEN Coalesce(books_diff.OPEN_ACCESS,  books_full.OPEN_ACCESS) = 0 THEN "N"
      WHEN Coalesce(books_diff.OPEN_ACCESS,
      books_full.OPEN_ACCESS) = 1 THEN "Y"
      ELSE NULL
    END BookOpenAccess,
    PARSE_DATE('%Y%m%d',
      Coalesce(books_diff.PLANNED_PUB_DATE,
        books_full.PLANNED_PUB_DATE)) BookPlannedPublicationDAte,
    UPPER(Coalesce(books_diff.PLANNING_HINT,
        books_full.PLANNING_HINT)) BookPlanningHint,
    CASE
      WHEN Coalesce(books_diff.POD_ACTIVE,  books_full.POD_ACTIVE) = 0 THEN "N"
      WHEN Coalesce(books_diff.POD_ACTIVE,
      books_full.POD_ACTIVE) = 1 THEN "Y"
      ELSE NULL
    END BookPODActive,
    Coalesce(books_diff.NO_POD_SUIT_REAS,
      books_full.NO_POD_SUIT_REAS) BookPODnotsuitablereason,
    CASE
      WHEN Coalesce(books_diff.NO_POD_SUIT_REAS,  books_full.NO_POD_SUIT_REAS) IS NULL THEN "Y"
      ELSE "N "END BookPODesuitable,
    Coalesce(books_diff.PRICE_EUR,
      book_prices.PRICE_EUR) BookPriceEUR,
--    books_diff.PRICE_EUR,
    Coalesce(books_diff.PRICE_USD,
      book_prices.PRICE_USD) BookPriceUSD,
--    books_diff.PRICE_USD,
    Coalesce(books_diff.PRICING_CLUSTER,
      books_full.PRICING_CLUSTER) BookPricingCluster,
    Coalesce(books_diff.PRINTER,
      books_full.PRINTER) BookPrinter,
    Coalesce(books_diff.PRINTRUN_NOMINAL,
      books_full.PRINTRUN_NOMINAL) BookPrintRunNominal,
    Coalesce(books_diff.PRINTRUN_PLANNED,
      books_full.PRINTRUN_PLANNED) BookPrinterRunPlanned,
    SUBSTR(Coalesce(books_diff.PRODUCT_CATEGORY,
        books_full.PRODUCT_CATEGORY),4,250) BookCategory,
    Coalesce(books_diff.MEDIUM,
      books_full.MEDIUM) BookType,
    UPPER(Coalesce(books_diff.PRODUCTION_CAT,
        books_full.PRODUCTION_CAT)) BookProductionCategory,
    Coalesce(books_diff.PRODUCT_CONTACT,
      books_full.PRODUCT_CONTACT) BookProductionContactatSpringer,
    Coalesce(books_diff.PRODUCTIONEDITOR,
      books_full.PRODUCTIONEDITOR) BookProductionEditor,
    Coalesce(books_diff.PRODUCTIONPROCES,
      books_full.PRODUCTIONPROCES) BookProductionProcess,
    PARSE_DATE('%Y%m%d',
      Coalesce(books_diff.LOG_INITIATED,
        books_full.LOG_INITIATED)) BookProjectInitiated,
    PARSE_DATE('%Y%m%d',
      Coalesce(books_diff.LOG_PROPOSAL_APPROVED,
        books_full.LOG_PROPOSAL_APPROVED)) BookProposalApproved,
    Coalesce(books_diff.PROPOSAL_STATUS,
      books_full.PROPOSAL_STATUS) BookProposalStatus,
    CASE
      WHEN Coalesce(books_diff.PODONLY,  books_full.PODONLY) = 0 THEN "N"
      WHEN Coalesce(books_diff.PODONLY,
      books_full.PODONLY) = 1 THEN "Y"
      ELSE NULL
    END BookPTO,
    PARSE_DATE('%Y%m%d',
      Coalesce(books_diff.PUB_DAT_ACT_1ST,
        books_full.PUB_DAT_ACT_1ST)) BookPubDateActual1stRelease,
    PARSE_DATE('%Y%m%d',
      Coalesce(books_diff.PUB_DAT_PLAN_1ST,
        books_full.PUB_DAT_PLAN_1ST)) BookPubDatePlanned1stRelease,
    SUBSTR(Coalesce(books_diff.PUBLIC_CLASS,
        books_full.PUBLIC_CLASS),3) BookPublicatonClass,
    Coalesce(books_diff.PUB_DATE_FIXED,
      books_full.PUB_DATE_FIXED) BookPublicationDateFixed,
    Coalesce(books_diff.PUB_DT_FIX_REAS,
      books_full.PUB_DT_FIX_REAS) BookPublicationDateFixedReason,
    Coalesce(books_diff.PUBLISHER,
      books_full.PUBLISHER) BookPublisher,
    Coalesce(books_diff.RENDITION_NAME,
      books_full.RENDITION_NAME) BookRenditionName,
    Coalesce(books_diff.IMPRINT,
      books_full.IMPRINT) BookReportingViaProductionProcess,
    Coalesce(books_diff.RESP_PERS,
      books_full.RESP_PERS) BookResponsiblePerson,
    Coalesce(books_diff.SALES_HIGHLIGHT,
      books_full.SALES_HIGHLIGHT) BookSalesHighlight,
    Coalesce(books_diff.SPRINGER_PROJECTS,
      books_full.SPRINGER_PROJECTS) BookSBA,
    Coalesce(books_diff.SERIE_NUMBER,
      books_full.SERIE_NUMBER) BookSeriesNumber,
    Coalesce(books_diff.TYPE,
      books_full.TYPE) BookSet,
    Coalesce(books_diff.ORDER_NUMBER,
      books_full.ORDER_NUMBER) BookSPIN,
    UPPER(Coalesce(books_diff.SPRINGER_PROJECTS,
        books_full.SPRINGER_PROJECTS)) BookSpringerProjects,
    CASE
      WHEN Coalesce(books_diff.SPRINGER_REFERENC,  books_full.SPRINGER_REFERENC) = "0" THEN "N"
      WHEN Coalesce(books_diff.SPRINGER_REFERENC,
      books_full.SPRINGER_REFERENC) = "1" THEN "Y"
      ELSE NULL
    END BookSpringerReference,
    Coalesce(books_diff.STATUS,
      books_full.STATUS) BookStatus,
    Coalesce(books_diff.STATUS_SEC_LOC,
      books_full.STATUS_SEC_LOC) BookStatusSecLocation,
    Coalesce(books_diff.SUBJECT_COLLECTION,
      books_full.SUBJECT_COLLECTION) BookSubjectCollection,
    Coalesce(books_diff.SUBJECT1,
      books_full.SUBJECT1) BookSubject1,
    Coalesce(books_diff.SUBJECT2,
      books_full.SUBJECT2) BookSubject2,
    Coalesce(books_diff.SUBJECT3,
      books_full.SUBJECT3) BookSubject3,
    Coalesce(books_diff.SUBJECT4,
      books_full.SUBJECT4) BookSubject4,
    Coalesce(books_diff.SUBJECT5,
      books_full.SUBJECT5) BookSubject5,
    Coalesce(books_diff.SUBJECT6,
      books_full.SUBJECT6) BookSubject6,
    Coalesce(books_diff.TITLE,
      books_full.TITLE) BookTitle,
    Coalesce(books_diff.TRIM_HEIGHT_MM,
      books_full.TRIM_HEIGHT_MM) BookTrimHeight,
    Coalesce(books_diff.TRIM_WIDTH_MM,
      books_full.TRIM_WIDTH_MM) BookTrimWidth,
    UPPER(Coalesce(books_diff.TSPLUS_EDITOR,
        books_full.TSPLUS_EDITOR)) BookTSEditor,
    Coalesce(books_diff.TYPESETTER,
      books_full.TYPESETTER) BookTypesetter,
    Coalesce(books_diff.TYPESET_LAYOUT,
      books_full.TYPESET_LAYOUT) BookTypesettingLayout,
    Coalesce(books_diff.VOLUME_NUMBER,
      books_full.VOLUME_NUMBER) BookVolumeNumber,
    Coalesce(books_diff.PSP_ELEMENT,
      books_full.PSP_ELEMENT) BookWBSElement,
    UPPER(Coalesce(books_diff.XPM,
        books_full.XPM)) BookxPM,
    CAST(Coalesce(books_diff.RESPONS_PERSON,
      books_full.RESPONS_PERSON) AS INT64) PS_CODE,
    Coalesce(books_diff.LASTUPLOADDATE,
      books_full.LASTUPLOADDATE) BookLastUploadDate
  FROM
    bk_books bk
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.bflux_books_diff` books_diff
  ON
    bk.BookKey = books_diff.ORDER_NUMBER
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.bflux_books_full` books_full
  ON
    bk.BookKey = books_full.ORDER_NUMBER 
LEFT JOIN book_prices 
ON bk.BookKey = book_prices.ORDER_NUMBER)
SELECT
  c.*,
gpu.SOURCE_ENTITY BookSourceEntity,
gpu.REPORTING_KEY  BookReportingKey,
gpu.PS_RESPONSIBLE  BookPS,
gpu.PS_TEXT BookPSDesc,
gpu.PD_RESPONSIBLE  BookPD,
gpu.PD_CODE BookPDCode,
gpu.PD_TEXT BookPDDesc,
gpu.PU_CODE BookPUCode,
gpu.PU_RESPONSIBLE BookPU,
gpu.PU_TEXT BookPUDesc,
gpu.COMPANY_NR_TEXT   BookMainProductionSiteYN,
gpu.PROD_RESPONS_L1  BookArea,
gpu.PROD_RESPONS_L2 BookAreaL2,
sc.TERM BookSubjectCollectionDescr,
s.SERIES_TITLE BookSeriesTitle,
s.SERIES_PISSN BookSeriesPrintISSN,
s.SERIES_EIISN BookSeriesEIISN,
ebooktext.TERM BookEbookPackageDesc
FROM
  getcolumns c
LEFT JOIN
  `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_gpu` gpu
  ON
  c.PS_CODE = gpu.PS_CODE
left join
  `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_subject_collection` sc on c.BookSubjectCollection = sc.CODE
left join
  `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_series` s on c.BookSeriesNumber = s.SERIES_ID
left join
`{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_ebook_packages` ebooktext
ON c.BookEbookPackage = substr(ebooktext.CODE,5,5)
