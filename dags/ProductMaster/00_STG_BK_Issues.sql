  SELECT distinct
    CASE
      WHEN ISSUE_KIND_CODE = "ZR" THEN CONCAT(CAST(JOURNAL_KEY AS STRING),"_",CAST(PRICELIST_YEAR AS STRING),"_",CAST(VOLUME_NO AS STRING),"_",CAST(FIRST_ISSUE AS STRING),"-",CAST(LAST_ISSUE AS STRING),"_Issue")
      WHEN ISSUE_KIND_CODE = "SZ" THEN CONCAT(CAST(JOURNAL_KEY AS STRING),"_",CAST(PRICELIST_YEAR AS STRING),"_",CAST(VOLUME_NO AS STRING),"_S",CAST(FIRST_ISSUE AS STRING),"_Supplement")
      ELSE CONCAT(CAST(JOURNAL_KEY AS STRING),"_",CAST(PRICELIST_YEAR AS STRING),"_",CAST(VOLUME_NO AS STRING),"_",CAST(FIRST_ISSUE AS STRING),"_Issue")
    END IssueKey
  FROM
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jfw_issues`
  UNION DISTINCT
  SELECT distinct
    CASE
      WHEN issue_type = "2"
      THEN concat(split(OBJECT_NAME,"_")[offset (0)],"_",split(OBJECT_NAME,"_")[offset (1)],"_",split(OBJECT_NAME,"_")[offset (2)],"_S",split(OBJECT_NAME,"_")[offset (3)],"_",split(OBJECT_NAME,"_")[offset (4)])
      ELSE OBJECT_NAME
    END IssueKey
  FROM
  `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jwf_issue_data`
  UNION DISTINCT
  SELECT distinct
  CASE
    WHEN split(object_name,"_")[offset(4)] = "Supplement" THEN concat(split(object_name,"_")[offset(0)],"_",split(object_name,"_")[offset(1)],"_",split(object_name,"_")[offset(2)],"_S",split(object_name,"_")[offset(3)],"_",split(object_name,"_")[offset(4)])
    ELSE object_name
  END IssueKey
  FROM
  `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jwf_issue_stats`
  UNION DISTINCT
  SELECT distinct
      CASE
        WHEN ISSUE_TYPE = "Combined" THEN CONCAT(CAST(JOURNAL_ID AS STRING), "_", CAST(split(OBJECT_NAME, "_")[Offset(1)] AS STRING), "_", CAST(VOLUMEID AS STRING),"_",CAST(ISSUE_ID_START AS STRING),"-",CAST(ISSUE_ID_END AS STRING),"_Issue")
        WHEN ISSUE_TYPE = "Supplement" THEN CONCAT(CAST(JOURNAL_ID AS STRING),"_",CAST(split(OBJECT_NAME, "_")[Offset(1)] AS STRING),"_",CAST(VOLUMEID AS STRING),"_S",CAST(ISSUE_ID_START AS STRING),"_Supplement")
        ELSE CONCAT(CAST(JOURNAL_ID AS STRING),"_",CAST(split(OBJECT_NAME, "_")[Offset(1)] AS STRING),"_",CAST(VOLUMEID AS STRING),"_",CAST(ISSUE_ID_START AS STRING),"_Issue")
      END IssueKey
   FROM
  `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jflux_issue`
  UNION DISTINCT
  SELECT DISTINCT
      case
        when issue like 'S%' then CONCAT(cast(JOURNAL as string),"_", cast(ANNUAL as string),"_", VOLUME,"_",ISSUE,"_Supplement")
        else CONCAT(cast(JOURNAL as string),"_", cast(ANNUAL as string),"_", VOLUME,"_",ISSUE,"_Issue")
      end IssueKey
  FROM
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.dds_issues`
--- TO DO - Add distinct on all IssueKeys in Issue Hist dimension table to make sure we also include issue business keys from records that are not available in the source
