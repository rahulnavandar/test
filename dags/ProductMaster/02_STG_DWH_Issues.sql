WITH
  bk_issues AS (
  SELECT DISTINCT
    IssueKey
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_ProductMaster.BK_IssueDOI`),
  dds_issues AS (
  SELECT DISTINCT
    CONCAT(cast(JOURNAL as string),"_", cast(ANNUAL as string),"_", VOLUME,"_",ISSUE,"_Issue") IssueKey,
    *
  FROM
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.dds_issues` ),
  jfw_issues AS (
  SELECT DISTINCT
    CASE
      WHEN ISSUE_KIND_CODE = "ZR" THEN CONCAT(CAST(JOURNAL_KEY AS STRING),"_",CAST(PRICELIST_YEAR AS STRING),"_",CAST(VOLUME_NO AS STRING),"_",CAST(FIRST_ISSUE AS STRING),"-",CAST(LAST_ISSUE AS STRING),"_Issue")
      WHEN ISSUE_KIND_CODE = "SZ" THEN CONCAT(CAST(JOURNAL_KEY AS STRING),"_",CAST(PRICELIST_YEAR AS STRING),"_",CAST(VOLUME_NO AS STRING),"_S",CAST(FIRST_ISSUE AS STRING),"_Supplement")
      ELSE CONCAT(CAST(JOURNAL_KEY AS STRING),"_",CAST(PRICELIST_YEAR AS STRING),"_",CAST(VOLUME_NO AS STRING),"_",CAST(FIRST_ISSUE AS STRING),"_Issue")
    END IssueKey,
    *
  FROM
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jfw_issues` ),
  jwf_issue_data AS (
  SELECT DISTINCT
    CASE
      WHEN issue_type = "2"
      THEN concat(split(OBJECT_NAME,"_")[offset (0)],"_",split(OBJECT_NAME,"_")[offset (1)],"_",split(OBJECT_NAME,"_")[offset (2)],"_S",split(OBJECT_NAME,"_")[offset (3)],"_",split(OBJECT_NAME,"_")[offset (4)])
      ELSE OBJECT_NAME
    END IssueKey,
    *
  FROM
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jwf_issue_data` ),
  jwf_issue_stats AS (
  SELECT DISTINCT
    CASE
      WHEN split(object_name,"_")[offset(4)] = "Supplement" THEN concat(split(object_name,"_")[offset(0)],"_",split(object_name,"_")[offset(1)],"_",split(object_name,"_")[offset(2)],"_S",split(object_name,"_")[offset(3)],"_",split(object_name,"_")[offset(4)])
      ELSE object_name
    END IssueKey,
    *
  FROM
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jwf_issue_stats` ),
  jflux_issues as (
  SELECT DISTINCT
    CASE
      WHEN ISSUE_TYPE = "Combined" THEN CONCAT(CAST(JOURNAL_ID AS STRING), "_", CAST(split(OBJECT_NAME, "_")[Offset(1)] AS STRING), "_", CAST(VOLUMEID AS STRING),"_",CAST(ISSUE_ID_START AS STRING),"-",CAST(ISSUE_ID_END AS STRING),"_Issue")
      WHEN ISSUE_TYPE = "Supplement" THEN CONCAT(CAST(JOURNAL_ID AS STRING),"_",CAST(split(OBJECT_NAME, "_")[Offset(1)] AS STRING),"_",CAST(VOLUMEID AS STRING),"_S",CAST(ISSUE_ID_START AS STRING),"_Supplement")
      ELSE CONCAT(CAST(JOURNAL_ID AS STRING),"_",CAST(split(OBJECT_NAME, "_")[Offset(1)] AS STRING),"_",CAST(VOLUMEID AS STRING),"_",CAST(ISSUE_ID_START AS STRING),"_Issue")
    END IssueKey,
    *
  FROM
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jflux_issue` )
SELECT DISTINCT
  bk.IssueKey,
  CASE
    WHEN jfw_i.AVAILABLE = 0 then "N"
    WHEN jfw_i.AVAILABLE = 1 then "Y"
    ELSE NULL
  END IssueAvailable,
jfw_i.COST_UNIT IssueCostUnit,
coalesce(dds_i.COVER_DATE,
         jwf_s.cover_date) IssueCoverDate,
case when jfw_i.DELIVERY_DATE = '00010101' THEN cast(null as date)
     else PARSE_DATE('%Y%m%d',jfw_i.DELIVERY_DATE)
     end IssueDeliveryDate,
case when jfw_i.DISTRIBUTION_DATE = '00010101' THEN cast(null as date)
     else PARSE_DATE('%Y%m%d',jfw_i.DISTRIBUTION_DATE)
     end IssueDistributionDate,
case when jfw_i.IMPLEMENTATION_DATE = '00010101' THEN cast(null as date)
     else PARSE_DATE('%Y%m%d',jfw_i.IMPLEMENTATION_DATE)
     end IssueImplementationDate,
case when jfw_i.INIT_DISTR_DATE = '00010101' THEN cast(null as date)
     else PARSE_DATE('%Y%m%d',jfw_i.INIT_DISTR_DATE)
     end IssueInitDistrDate,
coalesce(jfw_i.LAST_ISSUE,
         jflux_i.issue_id_end,
         cast(jwf_d.issueidend as INT64),
         case
           when dds_i.issue like '%-%' then cast(split(dds_i.issue,"-")[offset(1)] as int64)
           else cast(dds_i.issue as int64)
         end) IssueIDEnd,
coalesce(jfw_i.FIRST_ISSUE,
         jflux_i.issue_id_start,
         cast(jwf_d.issueidstart as int64),
         case
           when dds_i.issue like '%-%' then cast(split(dds_i.issue,"-")[offset(0)] as int64)
           else cast(dds_i.issue as int64)
         end) IssueIDStart,
coalesce(jfw_i.first_page, jflux_i.issue_first_page) IssueFirstPage,
coalesce(jfw_i.last_page, jflux_i.issue_last_page) IssueLastPage,
coalesce(PARSE_DATE('%Y%m%d', jfw_i.LAST_ACTION_DATE),
         PARSE_DATE('%Y%m%d', jflux_i.last_action_date)) IssueModificationDate,
Coalesce(cast((CASE WHEN jfw_i.ISSUE_KIND_CODE = "ZR" THEN concat(cast(jfw_i.FIRST_ISSUE as string) , "-", CAST(jfw_i.LAST_ISSUE AS STRING))
                    WHEN jfw_i.ISSUE_KIND_CODE = "SZ" THEN CONCAT("S", CAST(jfw_i.FIRST_ISSUE AS STRING))
                    ELSE CAST(jfw_i.FIRST_ISSUE AS STRING)
               end) as string)  ,
         cast(CASE WHEN jflux_i.ISSUE_TYPE = "Combined" THEN concat(cast(jflux_i.ISSUE_ID_START as string) , "-", CAST(jflux_i.ISSUE_ID_END AS STRING))
                   WHEN jflux_i.ISSUE_TYPE = "Supplement" THEN CONCAT("S", CAST(jflux_i.ISSUE_ID_START AS STRING))
                   ELSE CAST(jflux_i.ISSUE_ID_START AS STRING)
              end as string) ,
         cast(dds_i.ISSUE as string),
         CASE
           WHEN jwf_d.issue_type = "2"
             THEN concat(split(jwf_d.OBJECT_NAME,"_")[offset (0)],"_",split(jwf_d.OBJECT_NAME,"_")[offset (1)],"_",split(jwf_d.OBJECT_NAME,"_")[offset (2)],"_S",split(jwf_d.OBJECT_NAME,"_")[offset (3)],"_",split(jwf_d.OBJECT_NAME,"_")[offset (4)])
             ELSE jwf_d.OBJECT_NAME
           END,
         CASE
           WHEN split(jwf_s.object_name,"_")[offset(4)] = "Supplement" THEN concat(split(jwf_s.object_name,"_")[offset(0)],"_",split(jwf_s.object_name,"_")[offset(1)],"_",split(jwf_s.object_name,"_")[offset(2)],"_S",split(jwf_s.object_name,"_")[offset(3)],"_",split(jwf_s.object_name,"_")[offset(4)])
           ELSE jwf_s.object_name
         END) IssueNumber,
coalesce(case
           when jfw_i.ISSUE_KIND_CODE = "SZ" then cast("2" as string)
           else cast("1" as string)
           end,
         jwf_d.issue_type,
         case
           when jflux_i.ISSUE_TYPE = "Supplement" then cast("2" as string)
           else cast("1" as string)
           end) IssueType,
Coalesce(jfw_i.JOURNAL_KEY, jflux_i.journal_id,
         cast(dds_i.JOURNAL as INT64),
         cast( split(jwf_d.object_name,"_")[offset(0)] as INT64),
         jflux_i.journal_id) IssueJournalTitleNumber,
coalesce(jfw_i.KIND_ISSUE_TEXT,
         case
           when jwf_d.issue_type = "2" then "Supplement"
           when jwf_d.issueidstart <> jwf_d.issueidend and jwf_d.issue_type = "1" then "Multiple issue"
           else "Issue"
         end,
         case
           when jflux_i.issue_type = "Combined" then "Multiple issue"
           when jflux_i.issue_type = "Regular" then "Issue"
           else jflux_i.issue_type
         end) IssueKindofIssue,
jfw_i.ARTICLES_PER_YEAR IssueNbrofArticlesYear,
jfw_i.ISSUES_PER_YEAR IssueNbrofIssuesYear,
--jfw_i.FIRST_ISSUE IssueNumberofIssues,
figures.NumberOfIssues IssueNumberofIssues,
figures.NumberOfIssuePages IssueNumberOfPages,
--sum( jflux_i.ISSUE_LAST_PAGE - jflux_i.ISSUE_FIRST_PAGE + 1) OVER (PARTITION BY jflux_i.OBJECT_NAME) IssueNumberofPages,
--jfw_i.ISSUE_KIND_CODE IssueObjectname,
coalesce(CASE WHEN jfw_i.ISSUE_KIND_CODE = "ZR"
                THEN CONCAT(CAST(jfw_i.JOURNAL_KEY AS STRING),"_",CAST(jfw_i.PRICELIST_YEAR AS STRING),"_",CAST(jfw_i.VOLUME_NO AS STRING),"_",CAST(jfw_i.FIRST_ISSUE AS STRING),"-",CAST(jfw_i.LAST_ISSUE AS STRING),"_Issue")
              WHEN jfw_i.ISSUE_KIND_CODE = "SZ"
                THEN CONCAT(CAST(jfw_i.JOURNAL_KEY AS STRING),"_",CAST(jfw_i.PRICELIST_YEAR AS STRING),"_",CAST(jfw_i.VOLUME_NO AS STRING),"_S",CAST(jfw_i.FIRST_ISSUE AS STRING),"_Supplement")
              ELSE CONCAT(CAST(jfw_i.JOURNAL_KEY AS STRING),"_",CAST(jfw_i.PRICELIST_YEAR AS STRING),"_",CAST(jfw_i.VOLUME_NO AS STRING),"_",CAST(jfw_i.FIRST_ISSUE AS STRING),"_Issue")
         END,
         CASE WHEN jflux_i.ISSUE_TYPE = "Supplement"
              THEN CONCAT(CAST(jflux_i.JOURNAL_ID AS STRING),"_",CAST(split(jflux_i.object_name,"_")[offset (1)] AS STRING),"_",CAST(jflux_i.VOLUMEID AS STRING),"_S",CAST(jflux_i.ISSUE_ID_START AS STRING),"_Supplement")
              ELSE jflux_i.object_name
         END,
         CASE
           WHEN jwf_d.issue_type = "2"
             THEN CONCAT(CAST(split(jwf_d.object_name,"_")[offset(0)] AS STRING),"_",CAST(split(jwf_d.object_name,"_")[offset (1)] AS STRING),"_",CAST(split(jwf_d.object_name,"_")[offset(2)] AS STRING),"_S",CAST(jwf_d.ISSUEIDSTART AS STRING),"_Supplement")
           ELSE jwf_d.object_name
         END,
         CASE
           WHEN split(jwf_s.object_name,"_")[offset(4)] = "Supplement" THEN concat(split(jwf_s.object_name,"_")[offset(0)],"_",split(jwf_s.object_name,"_")[offset(1)],"_",split(jwf_s.object_name,"_")[offset(2)],"_S",split(jwf_s.object_name,"_")[offset(3)],"_",split(jwf_s.object_name,"_")[offset(4)])
           ELSE jwf_s.object_name
         END) IssueObjectName,
coalesce(case when jfw_i.PUBLISHED_ONLINE = '00010101'
              THEN cast(null as date)
              else PARSE_DATE('%Y%m%d',jfw_i.PUBLISHED_ONLINE)
         end,
         parse_date('%Y%m%d',jflux_i.online_date),
         case when jwf_d.ONLINE_DATE = '00010101'
               THEN cast(null as date)
               else PARSE_DATE('%Y%m%d',jwf_d.ONLINE_DATE)
         end,
         case when jwf_s.ONLISSUEPUBL = '00010101'
               THEN cast(null as date)
               else PARSE_DATE('%Y%m%d',jwf_s.ONLISSUEPUBL)
         end) IssueOnlineIssuePublished,
coalesce(CASE WHEN jfw_i.PHYSICAL = 0 then "N"
              WHEN jfw_i.PHYSICAL = 1 then "Y"
              else NULL
         END,
         CASE WHEN jflux_i.PHYSICAL = 0 then "N"
              WHEN jflux_i.PHYSICAL = 1 then "Y"
              else NULL
         END,
         jwf_d.physical,
         jwf_s.physical,
         dds_i.physical) IssuePhysical,
jfw_i.PLANNED_PAGES IssuePlannedNoofPages,
case when jfw_i.PLANNED_PUB_DATE = '00010101' THEN cast (null as date)
     else PARSE_DATE('%Y%m%d',jfw_i.PLANNED_PUB_DATE)
     end IssuePlannedPublicationDate,
jfw_i.POD_UNSUIT_REAS IssuePODnotSuitableReason,
CASE WHEN jfw_i.POD_SUITABLE = 0 then "N"
     WHEN jfw_i.POD_SUITABLE = 1 then "Y"
     else NULL END  IssuePODeSuitable,
Coalesce(Cast(jfw_i.PRICELIST_YEAR as STRING),
         split(jwf_d.object_name,"_")[offset(1)],
         split(jwf_s.object_name,"_")[offset(1)],
         split(jflux_i.OBJECT_NAME, "_")[Offset(1)],
         cast(dds_i.ANNUAL as string)) IssuePriceListYear,
CASE WHEN jfw_i.PRINT_PDF_AVAIL = 0 then "N"
     WHEN jfw_i.PRINT_PDF_AVAIL = 1 then "Y"
     else NULL END  IssuePrintPDFAvailable,
jfw_i.PRINTRUN_MARKETING IssuePrintRunMarketing,
jfw_i.PRINTRUN_OTHERS IssuePrintRunOthers,
jfw_i.PRINTRUN_REGULAR IssuePrintRunRegular,
case when jfw_i.PRODUCTION_DATE = '00010101' then cast (null as date)
     else PARSE_DATE('%Y%m%d',jfw_i.PRODUCTION_DATE)
     end IssueProductionDate,
Upper(jfw_i.PRODUCTION_SITE) IssueProductionSite,
coalesce(jflux_i.production_system,
         jwf_d.Production_System,
         jwf_s.production_system) IssueProductionSystem,
parse_date('%Y%m%d',dds_i.ONLINE_DATE) IssuePublishedOnlineDateDDS,
jfw_i.PUBLISHER IssuePublisher,
coalesce(parse_date('%Y%m%d',jflux_i.readyissuebuild),
         parse_date('%Y%m%d',jwf_s.readyissuebuild)) IssueReadyForIssueBuilding,
coalesce (parse_date('%Y%m%d',jflux_i.productionwfstart),
          parse_date('%Y%m%d',jwf_s.prodwflowstart)) IssueProductionWFStart,
Upper(jfw_i.SALES_FORMAT) IssueSalesFormat,
coalesce(parse_date('%Y%m%d',jflux_i.SENTISSUETYPESETT),
         parse_date('%Y%m%d',jwf_s.SENTISSTYPESET)) IssueSentForIssueTypesetting,
coalesce(parse_date('%Y%m%d',jflux_i.INISSUETYPESETT),
         parse_date('%Y%m%d',jwf_s.INISSUETYPESET)) IssueInIssueTypesetting,
coalesce(parse_date('%Y%m%d',jflux_i.ISSUETYPESETTDON),
         parse_date('%Y%m%d',jwf_s.ISSTYPESETDONE)) IssueTypesettingDone,
coalesce(parse_date('%Y%m%d',jflux_i.CHECKQUALITYISS),
         parse_date('%Y%m%d',jwf_s.CHECKQUALITYISS)) IssueCheckQuality,
coalesce(parse_date('%Y%m%d',jflux_i.ISSQUALITYDISAPP),
         parse_date('%Y%m%d',jwf_s.ISSQUALDISAPP)) IssueQualityCheckDisapproved,
coalesce(parse_date('%Y%m%d',jflux_i.ISSUEQUALITYAPPR),
         parse_date('%Y%m%d',jwf_s.ISSUEQUALAPP)) IssueQualityCheckApproved,
coalesce(parse_date('%Y%m%d',jflux_i.senttoprinter),
         parse_date('%Y%m%d',jwf_s.senttoprinter)) IssueSentToPrinter,
coalesce(parse_date('%Y%m%d',jflux_i.PREPONLINEISSPUB),
         parse_date('%Y%m%d',jwf_s.PREPONLISSPUB)) IssuePrepareOnlineIssuePublished,
coalesce(parse_date('%Y%m%d',jflux_i.sendingtodds),
         parse_date('%Y%m%d',jwf_s.sendingtodds)) IssueSendingToDDS,
coalesce(parse_date('%Y%m%d',jflux_i.senttodds),
         parse_date('%Y%m%d',jwf_s.senttodds)) IssueSentToDDS,
coalesce(parse_date('%Y%m%d',jflux_i.receivedbydds),
         parse_date('%Y%m%d',jwf_s.receivedbydds)) IssueReceivedByDDS,
jflux_i.jnl_printer_name IssueJournalPrinter,
coalesce(jflux_i.current_state,
         jwf_d.r_current_state) IssueCurrentStatus,
coalesce(jfw_i.TITLE,
         jflux_i.issue_title) IssueTitle,
cast(coalesce(cast(jfw_i.VOLUME_NO as string),
         cast(jflux_i.volumeid as string),
         split(jwf_d.object_name,"_")[offset(2)],
         split(jwf_s.object_name,"_")[offset(2)],
         dds_i.volume) as string) IssueVolume,
cast(coalesce(cast(jfw_i.VOLUME_NO as string),
         cast(jflux_i.volumeid as string),
         split(jwf_d.object_name,"_")[offset(2)],
         split(jwf_s.object_name,"_")[offset(2)],
         dds_i.volume) as string) IssueVolumeNumber,
Coalesce(cast(jfw_i.VOLUME_NO AS STRING),
         cast(jflux_i.volumeid as string),
         dds_i.VOLUME,
         split(jwf_d.object_name,"_")[offset(2)],
         split(jwf_s.object_name,"_")[offset(2)] ) IssueVolumeNumberIssue,
--jfw_i.JOURNAL_KEY IssueWBSElement,
concat("S", cast(gpu.SOURCE_ENTITY as string), cast(jfw_i.JOURNAL_KEY as string), cast(jfw_i.VOLUME_NO as string), "000", cast(jfw_i.FIRST_ISSUE as string)) as IssueWBSElement,
Coalesce(jfw_i.LASTUPLOADDATE,
         jflux_i.lastuploaddate,
         jwf_d.lastuploaddate,
         jwf_s.lastuploaddate,
         dds_i.LASTUPLOADDATE) IssueLastUploadDate,
coalesce(jflux_i.no_of_art_pages,
         jwf_d.article_pages) IssueNumberOfArticlePages,
coalesce(case
           when split(jflux_i.object_name,"_")[offset (4)] = 'Issue' then concat(split(jflux_i.object_name,"_")[offset (2)], "/", split(jflux_i.object_name,"_")[offset (3)])
           when split(jflux_i.object_name,"_")[offset (4)] = 'Supplement' then concat(split(jflux_i.object_name,"_")[offset (2)], "/", "S",split(jflux_i.object_name,"_")[offset (3)])
         end,
         case
           when split(jwf_d.object_name,"_")[offset (4)] = 'Issue' then concat(split(jwf_d.object_name,"_")[offset (2)], "/", split(jwf_d.object_name,"_")[offset (3)])
           when split(jwf_d.object_name,"_")[offset (4)] = 'Supplement' then concat(split(jwf_d.object_name,"_")[offset (2)], "/", "S",split(jwf_d.object_name,"_")[offset (3)])
         end,
         case
           when split(jwf_s.object_name,"_")[offset (4)] = 'Issue' then concat(split(jwf_s.object_name,"_")[offset (2)], "/", split(jwf_s.object_name,"_")[offset (3)])
           when split(jwf_s.object_name,"_")[offset (4)] = 'Supplement' then concat(split(jwf_s.object_name,"_")[offset (2)], "/", "S",split(jwf_s.object_name,"_")[offset (3)])
         end) IssueVolumeIssueNr,
coalesce(concat("S", cast(gpu.SOURCE_ENTITY as string), cast(jfw_i.JOURNAL_KEY as string), cast(jfw_i.VOLUME_NO as string), cast(jfw_i.FIRST_ISSUE as string),"000"),
         replace(replace(jflux_i.wbs_element,"/",""),".","")) IssuePSPElement,
coalesce(jfw_i.issue_kind_code,
         case
           when jwf_d.issue_type = '2' then 'SZ'
           when jwf_d.issue_type = '1' and jwf_d.issueidstart <> jwf_d.issueidend then 'ZR'
           else 'NH'
         end,
         case
           when jflux_i.issue_type = 'Supplement' then 'SZ'
           when jflux_i.issue_type = 'Combined' then 'ZR'
           else 'NH'
         end) IssueTypeCode,
coalesce(jflux_i.binding,
         jwf_s.binding) IssueBinding,
coalesce(jflux_i.TEXT_PAPERSTOCK,
         jwf_s.text_paperstock) IssuePaperStockText,
coalesce(jflux_i.COVER_PRINTING,
         jwf_s.COVER_PRINTING) IssueCoverPrinting,
coalesce(jflux_i.COVER_PAPERSTOCK,
         jwf_s.COVER_PAPERSTOCK) IssueCoverPaperStock,
coalesce(jflux_i.COVER_FINISHING,
         jwf_s.COVER_FINISHING) IssueCoverProductionInfoFinishing,
coalesce(jflux_i.JNL_PRINT_QUALIT,
         jwf_s.JNL_PRINT_QUALIT) IssueJournalPrintQuality,
jwf_d.other_a_pages IssueOtherAPages,
jwf_d.addition_pages IssueAdditionalPages,
jwf_d.contentinjwf IssueContentInJWF,
jwf_s.weight IssueWeight,
jwf_s.editorial_pages IssueEditorialPages,
parse_date('%Y%m%d',jwf_s.DISTRWFSTARTED) IssueDistributionWFStart,
parse_date('%Y%m%d',jwf_s.atprinter) IssueAtPrinter,
parse_date('%Y%m%d',jwf_s.printedissship) IssuePrintedShipped
FROM
  bk_issues bk
LEFT JOIN
  dds_issues dds_i
ON
  bk.IssueKey = dds_i.IssueKey
LEFT JOIN
  jfw_issues jfw_i
ON
  bk.IssueKey = jfw_i.IssueKey
LEFT JOIN
  jwf_issue_data jwf_d
ON
  bk.IssueKey = jwf_d.IssueKey
LEFT JOIN
  jwf_issue_stats jwf_s
ON
  bk.IssueKey = jwf_s.IssueKey
LEFT JOIN
  jflux_issues jflux_i
ON
  bk.IssueKey = jflux_i.IssueKey
LEFT JOIN
  `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_gpu` gpu
ON
  jfw_i.PS = gpu.PS_CODE
LEFT JOIN
  `{{ params.project }}.{{ params.environment }}_STG_ProductMaster.BK_IssuesKeyFigures` figures
ON
  bk.IssueKey = figures.IssueKey