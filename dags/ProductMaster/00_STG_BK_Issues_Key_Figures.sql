 SELECT DISTINCT
   CASE
     WHEN ISSUE_TYPE = "Combined" THEN CONCAT(CAST(JOURNAL_ID AS STRING), "_", CAST(split(OBJECT_NAME, "_")[Offset(1)] AS STRING), "_", CAST(VOLUMEID AS STRING),"_",CAST(ISSUE_ID_START AS STRING),"-",CAST(ISSUE_ID_END AS STRING),"_Issue")
     WHEN ISSUE_TYPE = "Supplement" THEN CONCAT(CAST(JOURNAL_ID AS STRING),"_",CAST(split(OBJECT_NAME, "_")[Offset(1)] AS STRING),"_",CAST(VOLUMEID AS STRING),"_S",CAST(ISSUE_ID_START AS STRING),"_Supplement")
     ELSE CONCAT(CAST(JOURNAL_ID AS STRING),"_",CAST(split(OBJECT_NAME, "_")[Offset(1)] AS STRING),"_",CAST(VOLUMEID AS STRING),"_",CAST(ISSUE_ID_START AS STRING),"_Issue")
   END IssueKey,
   sum( ISSUE_ID_END - ISSUE_ID_START + 1) as NumberOfIssues,
   sum( ISSUE_LAST_PAGE - ISSUE_FIRST_PAGE + 1) as NumberOfIssuePages
 FROM
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jflux_issue`
 group by OBJECT_NAME, ISSUE_ID_START, ISSUE_ID_END, issue_type, journal_id, volumeid
 UNION DISTINCT
 SELECT DISTINCT
   CASE
     WHEN ISSUE_KIND_CODE = "ZR" THEN CONCAT(CAST(JOURNAL_KEY AS STRING),"_",CAST(PRICELIST_YEAR AS STRING),"_",CAST(VOLUME_NO AS STRING),"_",CAST(FIRST_ISSUE AS STRING),"-",CAST(LAST_ISSUE AS STRING),"_Issue")
     WHEN ISSUE_KIND_CODE = "SZ" THEN CONCAT(CAST(JOURNAL_KEY AS STRING),"_",CAST(PRICELIST_YEAR AS STRING),"_",CAST(VOLUME_NO AS STRING),"_S",CAST(FIRST_ISSUE AS STRING),"_Supplement")
     ELSE CONCAT(CAST(JOURNAL_KEY AS STRING),"_",CAST(PRICELIST_YEAR AS STRING),"_",CAST(VOLUME_NO AS STRING),"_",CAST(FIRST_ISSUE AS STRING),"_Issue")
   END IssueKey,
   sum( LAST_ISSUE - FIRST_ISSUE + 1) as NumberOfIssues,
   sum(last_page - first_page + 1) as NumberOfPages
 FROM
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jfw_issues`
 group by ISSUE_KIND_CODE,JOURNAL_KEY,PRICELIST_YEAR,VOLUME_NO,FIRST_ISSUE,LAST_ISSUE
 UNION DISTINCT
 SELECT DISTINCT
   CASE
     WHEN issue_type = "2"
     THEN concat(split(OBJECT_NAME,"_")[offset (0)],"_",split(OBJECT_NAME,"_")[offset (1)],"_",split(OBJECT_NAME,"_")[offset (2)],"_S",split(OBJECT_NAME,"_")[offset (3)],"_",split(OBJECT_NAME,"_")[offset (4)])
     ELSE OBJECT_NAME
   END IssueKey,
   sum( cast(ISSUEIDEND as int64) - cast(ISSUEIDSTART as int64) + 1) as NumberOfIssues,
   article_pages as NumberOfPages
 FROM
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jwf_issue_data`
 group by OBJECT_NAME,ISSUE_TYPE,ARTICLE_PAGES