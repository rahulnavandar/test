WITH
  getarticledoi AS (
  SELECT
    Upper(DOI) DOI
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_ProductMaster.BK_ArticleDOI`),
  getcolumns AS (
  SELECT
--    bk.UsageItemDOI,
    bk.DOI,
    jflux.NOOFCOLIMAGESONPAG ArticleNbrofColoredPages,
    coalesce(jflux.FREE_EOFFPRINT,
             jwf_offprints.FREE_E_OFFPRINTS) ArticleNbrofFreeEoffPrints,
    coalesce(jflux.NR_FREE_OFFPRINT,
             jwf_offprints.FREE_OFFPRINTS) ArticleNbrFreeoffprints,
    coalesce(jflux.PAID_EOFFPRINT,
             jwf_offprints.PAID_E_OFFPRINTS) ArticleNbrofPaidEoffPrints,
    coalesce(jflux.NR_PAID_OFFPRINT,
             jwf_offprints.PAID_OFFPRINTS) ArticleNbrofPaidOffprints,
    jwf_offprints.POSTERS,
    coalesce(PARSE_DATE('%Y%m%d', jflux.ACCEPTED_DATE),
             PARSE_DATE('%Y%m%d', jwf.ARTICLE_ACCEPTED)) ArticleAccepted,
    COALESCE(dds_diff.ARTICLECATEGORY,
             dds_full.ARTICLECATEGORY,
             jflux.ARTICLE_CATEGORY,
             jwf.article_category,
             magnus.SARTCATEG) ArticleCategory,
    COALESCE(dds_diff.ARTICLE_ID,
             dds_full.ARTICLE_ID,
             jflux.ARTICLEID,
             jwf.ARTICLEID,
             dds_miss.ArtID) ArticleID,
    coalesce(PARSE_DATE('%Y%m%d', jflux.LAST_ACTION_DATE),
             parse_date('%Y%m%d', jwf.art_change_date),
             PARSE_DATE('%Y%m%d', jwf_offprints.LAST_ACTION_DATE)) ArticleModificationDate,
    coalesce(PARSE_DATE('%Y%m%d', jflux.REGISTRATION_DATE),
             PARSE_DATE('%Y%m%d', jwf.REGISTRATIONDATE)) ArticleRegistrationDate,
    UPPER(COALESCE(dds_diff.ARTICLE_TYPE,
                   dds_full.ARTICLE_TYPE,
                   jflux.ARTICLE_TYPE,
                   jwf.article_type,
                   dds_miss.ArtType)) ArticleType,
    COALESCE(dds_diff.ARTICLEAUTHOR,
             dds_full.ARTICLEAUTHOR,
             jflux.ARTICLE_AUTHOR,
             concat(author_firstname, ' ', author_lastname)) ArticleAuthor,
    UPPER(coalesce(jflux.ARTICLE_BODY_MARKUP,
                   jwf.body_markup)) ArticleBodyMarkup,
    coalesce(PARSE_DATE('%Y%m%d', jflux.CHECK_PC_QUALITY),
             PARSE_DATE('%Y%m%d', jwf.CHECKPCQUALITY)) ArticleCheckPCQuality,
    coalesce(PARSE_DATE('%Y%m%d', jflux.CHECK_PROOF_QUALITY),
             PARSE_DATE('%Y%m%d', jwf.CHECKPROOFQUAL)) ArticleCheckProofQuality,
    coalesce(jflux.ARTICLE_COPYEDITING_CATEGORY,
             jwf.COPYEDITINGCATEG) ArticleCopyEditingCategory,
    coalesce(PARSE_DATE('%Y%m%d', jflux.CREATION_DATE),
             PARSE_DATE('%Y%m%d', jwf.CREATIONDATE)) ArticleCreationDate,
    UPPER(coalesce(jflux.CURRENT_STATE,
                   jwf.r_current_state)) ArticleCurrentStatus,
    UPPER(Coalesce(dds_diff.DOI,
                   dds_full.DOI,
                   jflux.ARTICLEDOI,
                   jwf.ARTICLEDOI,
                   jwf_offprints.DOI,
                   dds_miss.ArtDOI)) ArticleDOI,
    CONCAT("https://dx.doi.org/",UPPER(Coalesce(dds_diff.DOI,
                                                dds_full.DOI,
                                                dds_miss.ArtDOI))) ArticleDOILink,
    PARSE_DATE('%Y%m%d', COALESCE(dds_diff.FIRSTCHECKINDATE,
                                  dds_full.FIRSTCHECKINDATE,
                                  replace(cast(dds_miss.FirstCheckin as string),'-',''))) ArticleFirstCheckInDate,
    Coalesce(dds_diff.FIRST_PAGE,
             dds_full.FIRST_PAGE,
             jflux.ARTICLEFIRSTPAGE,
             jwf.FIRSTPAGE,
             dds_miss.ArtFirstPage) ArticleFirstPage,
    coalesce(PARSE_DATE('%Y%m%d', jflux.IN_ISSUE_WORKFLOW),
             PARSE_DATE('%Y%m%d', jwf.INISSUEWORKFLOW)) ArticleInIssueWorkflow,
    coalesce(PARSE_DATE('%Y%m%d', jflux.IN_PREPARE_CONTENT),
             PARSE_DATE('%Y%m%d', jwf.INPREPARECONTENT)) ArticleInPrepareContent,
    coalesce(jflux.AUTHOR_EMAIL,
             jwf.author_email) ArticleInternetMailAddress,
    coalesce(cast(dds_full.journal as int64),
             cast(dds_diff.journal as int64),
             jflux.JOURNAL_ID,
             cast(jwf.JOURNAL as int64),
             magnus.SJSNR,
             dds_miss.JouID) ArticleJournalTitleNumber,
    PARSE_DATE('%Y%m%d', Coalesce(dds_diff.LASTCHECKINDATE,
                                  dds_full.LASTCHECKINDATE,
                                  replace(cast(dds_miss.LastCheckin as string),'-',''))) ArticleLastCheckInDateDDS,
    Coalesce(dds_diff.LAST_PAGE,
             dds_full.LAST_PAGE,
             jflux.ARTICLELASTPAGE,
             jwf.LASTPAGE,
             dds_miss.ArtLastPage) ArticleLastpage,
    coalesce(jflux.EDITORIALMANUSCRIPTNUMBER,
             jwf.EDIT_MANUS_NR)  ArticleManuscriptNumber,
    coalesce(jflux.NOTEFORPE,
             jwf.noteforpe) ArticleNoteforProdEditor,
    jflux.NOTE_FOR_ISSUE_BUILDING ArticleNoteonIssueBuilding,
    UPPER(coalesce(jflux.OBJECT_NAME,
                   jwf.OBJECT_NAME)) ArticleObjectName,
    CASE WHEN coalesce(jflux.OPEN_ACCESS_ARTICLE, jwf.OPEN_ACCESS) = 1
         THEN "Y"
         ELSE "N"
    end ArticleOpenAccess,
    coalesce(PARSE_DATE('%Y%m%d', jflux.PCQUALDISAPPR),
             PARSE_DATE('%Y%m%d', jwf.PCQUALITYDISAPPR)) ArticlePCQualityDisapproved,
    coalesce(PARSE_DATE('%Y%m%d', jflux.PERFORMING_PROOF_CORRECTIONS),
             PARSE_DATE('%Y%m%d', jwf.PERFORMPROOFCORR)) ArticlePerformProofCorrections,
    coalesce(PARSE_DATE('%Y%m%d', jflux.PREPARE_CONTENT_COMPLETED),
             PARSE_DATE('%Y%m%d', jwf.PREPCONTENTCOMPL)) ArticlePrepareContentComplete,
    coalesce(PARSE_DATE('%Y%m%d', jflux.PREPAREFORPUBLICATION),
             PARSE_DATE('%Y%m%d', jwf.PREPAREFORPUBL)) ArticlePrepareforPublication,
    Coalesce(dds_diff.ANNUAL,
             dds_full.ANNUAL,
             split(jflux.ISSUE_OBJECT_NAME, "_")[Offset(1)],
             split(jwf.ISSUE, "_")[Offset(1)],
             cast(dds_miss.VolYear as string)) ArticlePricelistYear,
    jflux.PRINTER_SPACEID ArticlePrinter,
    coalesce(PARSE_DATE('%Y%m%d',jflux.ARTICLE_PRODUCTION_DEADLINE),
             parse_date('%Y%m%d',jwf.production_deadl)) ArticleProductionDeadline,
    coalesce(PARSE_DATE('%Y%m%d', jflux.PRODUCTIONWORKFLOWSTARTED),
             PARSE_DATE('%Y%m%d', jwf.PRODWFSTART)) ArticleProductionWorkflowStarted,
    coalesce(PARSE_DATE('%Y%m%d', jflux.PROOF_QUALITY_APPROVED),
             PARSE_DATE('%Y%m%d', jwf.PROOFQUALITYAPPR)) ArticleProofQualityApproved,
    coalesce(PARSE_DATE('%Y%m%d', jflux.PROOF_QUALITY_DISAPPROVED),
             PARSE_DATE('%Y%m%d', jwf.PROOFQUALDISAPPR)) ArticleProofQualityDisapproved,
    PARSE_DATE('%Y%m%d', Coalesce(dds_diff.ONLINE_DATE,
                                  dds_full.ONLINE_DATE,
                                  replace(cast(magnus.SPUBLONLA as string),'-',''))) ArticlePublishedOnlineDateDDS,
    coalesce(PARSE_DATE('%Y%m%d', jflux.ONLINE_DATE),
             PARSE_DATE('%Y%m%d', jwf.PUBLISHEDONLINE)) ArticlePublishedOnlineDateJWF,
    coalesce(PARSE_DATE('%Y%m%d', jflux.DELIVERED_TO_DDS),
             PARSE_DATE('%Y%m%d', jwf.RECEIVEDBYDDS)) ArticleReceivedbyDDS,
    coalesce(case when jflux.RELEASE_PUBLICATION_DATE = '00010101'
                  THEN cast(null as date)
                  else PARSE_DATE('%Y%m%d',jflux.RELEASE_PUBLICATION_DATE)
                  end,
             case when jwf.article_rel_publ = '00010101'
                  THEN cast(null as date)
                  else PARSE_DATE('%Y%m%d',jwf.article_rel_publ)
                  end) ArticleReleaseDate,
    coalesce(jflux.STAGE150_STATE,
             jwf.s150_state) ArticleS150State,
    coalesce(PARSE_DATE('%Y%m%d', jflux.SENDING_PROOF),
             PARSE_DATE('%Y%m%d', jwf.SENDINGPROOF)) ArticleSendingProof,
    coalesce(PARSE_DATE('%Y%m%d', jflux.SENTFORPREPARECONTENT),
             PARSE_DATE('%Y%m%d', jwf.SENTPREPCONTENT)) ArticleSentforPrepareContent,
    coalesce(PARSE_DATE('%Y%m%d', jflux.SENT_TO_DDS),
             PARSE_DATE('%Y%m%d', jwf.SENTTODDS))  ArticleSenttoDDS,
    coalesce(jflux.ARTICLESEQUENCENUMBER, jwf.SEQUENCENUMBER) ArticleSequenceNumber,
    COALESCE(dds_diff.TITLE,
             dds_full.TITLE,
             jflux. ARTICLE_TITLE,
             jwf.TITLE,
             magnus.STITLEHF,
             dds_miss.ArtTitle_50) ArticleTitle,
    jflux.VENDOR_SPACEID ArticleVendor,
    Coalesce(dds_diff.VOLUME,
             dds_full.VOLUME,
             split(jflux.issue_object_name, "_")[Offset(2)],
             split(jwf.issue, "_")[Offset(2)],
             cast(dds_miss.VolID as string)) ArticleVolume,
--      jflux.ISSUE_OBJECT_NAME) ArticleVolume,
    CAST(Coalesce(dds_diff.JOURNAL,
                  dds_full.JOURNAL,
                  CAST(jflux.JOURNAL_ID AS string),
                  CAST(jwf.JOURNAL AS string),
                  cast(magnus.SJSNR as string),
                  cast(dds_miss.JouID as string)) AS INT64) ArticleJournal,
    Coalesce(dds_diff.ANNUAL,
             dds_full.ANNUAL,
             split(jflux.issue_object_name, "_")[Offset(1)],
             split(jwf.issue, "_")[Offset(1)],
             cast(dds_miss.VolYear as string)) ArticleAnnual,
--      '') ArticleAnnual,
    Coalesce(dds_diff.ISSUE,
             dds_full.ISSUE,
             split(jflux.issue_object_name, "_")[Offset(3)],
             split(jwf.issue, "_")[Offset(3)],
             dds_miss.IsuID) ArticleIssue,
--      jflux.ISSUE_OBJECT_NAME) ArticleIssue,
--    CONCAT(CAST(CAST(Coalesce(dds_diff.JOURNAL,
--            dds_full.JOURNAL,
--            CAST(jflux.JOURNAL_ID AS string)) AS INT64) AS string), "_", Coalesce(dds_diff.ANNUAL,
--        dds_full.ANNUAL,
--        ''),"_", Coalesce(dds_diff.VOLUME,
--        dds_full.VOLUME,
--        jflux.ISSUE_OBJECT_NAME),"_", Coalesce(dds_diff.ISSUE,
--        dds_full.ISSUE,
--        jflux.ISSUE_OBJECT_NAME),"_Issue") ArticleIssueKey,
    coalesce(concat(replace(LTRIM (replace(dds_full.journal,'0',' ')),' ','0'),"_",dds_full.annual,"_",dds_full.volume,"_",dds_full.issue),
             concat(replace(LTRIM (replace(dds_diff.journal,'0',' ')),' ','0'),"_",dds_diff.annual,"_",dds_diff.volume,"_",dds_diff.issue),
             jflux.ISSUE_OBJECT_NAME,
             jwf.ISSUE,
             concat(cast(jouid as string),'_',cast(volyear as string),'_',cast(volid as string),'_',isuid,'_',case when isutype = 'Regular' or isutype = 'Combined' then 'Issue'
                                                                                                                   when length(isuid) = 1 then 'Issue'
                                                                                                                   when length(isuid) = 2 then 'Supplement'
                                                                                                                   when length(isuid) > 2 then 'Issue'
                                                                                                                   else isutype
                                                                                                              end)) ArticleIssueKey,
    jflux.oc_orgname ArticleOCOrgname,
    Coalesce(jflux.LASTUPLOADDATE,
             jwf.lastuploaddate,
             jwf_offprints.LASTUPLOADDATE,
             dds_diff.LASTUPLOADDATE,
             dds_full.LASTUPLOADDATE) ArticleLastUploadDate,
    case when jflux.ISSUE_TYPE = 'Regular' or dds_miss.IsuType = 'Regular' THEN "Issue"
         when jflux.ISSUE_TYPE = 'Combined' or dds_miss.IsuType = 'Combined' THEN "Issue"
         when jflux.ISSUE_TYPE = 'Supplement' or dds_miss.IsuType = 'Supplement' THEN "Supplement"
         else null
    end ArticleIssueType,
    coalesce(date_diff(parse_date('%Y%m%d',jwf.PUBLISHEDONLINE), parse_date('%Y%m%d',jwf.REGISTRATIONDATE), day),
             date_diff(parse_date('%Y%m%d',jflux.ONLINE_date), parse_date('%Y%m%d',jflux.REGISTRATION_DATE), day) ) as ArticlePublishedOFdays
  FROM
    getarticledoi bk
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.dds_article_full` dds_full
  ON
--    bk.UsageItemDOI = UPPER(dds_full.DOI)
    bk.DOI = UPPER(dds_full.DOI)
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.dds_article_diff` dds_diff
  ON
--    bk.UsageItemDOI = UPPER(dds_diff.DOI)
    bk.DOI = UPPER(dds_diff.DOI)
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jflux_article` jflux
  ON
--    bk.UsageItemDOI = UPPER(jflux.ARTICLEDOI) )
    bk.DOI = UPPER(jflux.ARTICLEDOI)
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jwf_article` jwf
  ON
--    bk.UsageItemDOI = UPPER(jwf.ARTICLEDOI) )
    bk.DOI = UPPER(jwf.ARTICLEDOI)
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jwf_article_offprints` jwf_offprints
  ON
--    bk.UsageItemDOI = UPPER(jwf)offprints.DOI) )
    bk.DOI = UPPER(jwf_offprints.DOI)
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.magnus_products` magnus
  ON
    bk.DOI = UPPER(magnus.ITEMDOI)
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.dds_articles_missing` dds_miss
  ON
    bk.DOI = UPPER(dds_miss.ArtDOI)
)
SELECT
  *
FROM
  getcolumns