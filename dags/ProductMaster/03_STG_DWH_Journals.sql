with bk_journal as (
select DISTINCT JournalKey from `{{ params.project }}.{{ params.environment }}_STG_ProductMaster.BK_JournalDOI`
),
get_columns as (
select Cast(JournalKey as string) JournalKey,
--Coalesce(jwf_j.JOURNAL_KEY,jwf_n.PRODUCT_ID) JournalKey,
Coalesce(jwf_j.COPYRIGHT_HOLDER,jwf_n.COPYRIGHT_INFORMATION ) JournalCopyrightHolder,
Coalesce(jwf_j.COST_UNIT,jwf_n.PRODUCT_COST_UNIT_NUMBER) JournalCostUnit,
upper(jwf_j.JOURNAL_DOI) JournalDOI,
Coalesce(jwf_j.PS,jwf_n.PS_CODE ) JournalEntity,
jwf_j.FORMAT_TRIMSIZE JournalFormatTrimsize,
jwf_j.FULLY_OWNED JournalFullyOwndedbySpringer,
Coalesce(jwf_j.PS,jwf_n.PS_CODE) PS_CODE,
Coalesce(jwf_j.GROUP_OF_COMPANY,jwf_n.CODE_COMPANY_GROUP) JournalGroupofCompany,
Coalesce(jwf_j.IMPRINT,jwf_n.IMPRINT) JournalImprint,
Coalesce(jwf_j.ISSN_ELECTRONIC,jwf_n.ISSN_ELECTRONIC) JournalISSNElectronic,
Coalesce(jwf_j.ISSN_PRINT,jwf_n.ISSN_PRINT) JournalISSNPrint,
Upper(jwf_j.REFERENCE_KEY) JournalAbbreviation,
Cast(Coalesce(jwf_j.JOURNAL_KEY,jwf_n.PRODUCT_ID) as string) JournalTitleNo,
Coalesce(jwf_j.MAIN_DISCIPLINE,jwf_n.MAIN_DISCIPLINE_CODE) JournalMainDiscipline,
Coalesce(jwf_j.MAIN_LANGUAGE,jwf_n.PRIMARY_LANGUAGE_CODE) JournalMainLanugage,
Coalesce(jwf_j.MARKETING_TEAM,jwf_n.MARKETING_TEAM) JournalMarketingTeam,
jwf_j.ARTICLES_PER_YEAR JournalNbrOfArticlesYear,
jwf_j.NO_FREE_OFFPRINT JournalNbrofFreeOffPrints,
jwf_j.OJA JournalOnlineJRNLArchive,
jwf_j.OPEN_ACCESS_CODE JournalOpenAccessType,
Coalesce(jwf_j.PRINTER,jwf_n.PRINTER) JournalPrinter,
jwf_n.PROD_OBJ_TYPE JournalProductObjectType,
Coalesce(jwf_j.PRODUCT_CONTACT,jwf_n.PRODUCTION_CONTACTS) JournalProductionContactAtSpring,
jwf_j.PROD_COST_CENTER JournalCostCenter,
Coalesce(jwf_j.PROD_EDITOR ,jwf_n.PRODUCTION_EDITORS) JournalProductionEditor,
Upper(Coalesce(jwf_j.PRODUCTION_SITE,'JWF')) JournalProductionSite,
jwf_j.PROD_SYS_EXP_ACT JournalProductionSystemExportActivated,
Upper(Coalesce(jwf_j.FREQUENCY,jwf_n.FREQUENCY)) JournalPublicationFrequency,
Coalesce(jwf_j.JOURNAL_PUBLISHR,jwf_n.CODE_PUBLISHER) JournalPublisher,
Coalesce(jwf_j.PUBL_EDITOR,jwf_n.PUBLISHING_EDITORS) JournalPublishingEditor,
Coalesce(jwf_j.PS,jwf_n.PS_CODE) JournalReportingKey,
jwf_j.SHORT_TITLE JournalShortTitle,
Coalesce(jwf_j.SPRINGER_OPEN,jwf_n.SPRINGER_OPEN) JournalSpringerOpen,
Upper(Coalesce(jwf_j.STATUS,jwf_n.PRODUCT_STATUS)) JournalStatus,
jwf_j.SUBJECT_COLLECTION JournalSubjectCollection,
jwf_j.SUBJECT1 JournalSubject1,
jwf_n.SUBJECT1 JournalSubject2,
Coalesce(jwf_j.JOURN_SUBTIT,jwf_n.SUBTITLE) JournalSubTitle,
Coalesce(jwf_j.JOURNAL_TITLE,jwf_n.PRODUCT_TITLE) JournalTitle,
Coalesce(jwf_j.TOTAL_SERVICE,"NA") JournalTotalService,
Coalesce(jwf_j.TYPESETTER,jwf_n.TYPESETTER) JournalTypeSetter,
Coalesce(jwf_j.LASTUPLOADDATE,cast(jwf_n.LASTUPLOADDATE as date)) JournalLastUploadDate
from bk_journal bk
left join `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jfw_journals` jwf_j on bk.JournalKey = jwf_j.JOURNAL_KEY
left join  `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jfw_non_regular_journals` jwf_n on bk.JournalKey =  jwf_n.PRODUCT_ID
)
select c.*,
gpu.PD_CODE  JournalPDCode,
gpu.PD_TEXT JournalPDDesc,
gpu.PD_RESPONSIBLE   JournalPD,
gpu.PS_CODE  JournalPSCode,
gpu.PS_RESPONSIBLE   JournalPS,
gpu.PS_TEXT JournalPSDesc,
gpu.PU_CODE  JournalPUCode,
gpu.PU_RESPONSIBLE   JournalPU,
gpu.PU_TEXT JournalPUDesc,
sc.TERM JournalSubjectCollectionDescription,
pc.PRICE_EUR JournalPrice_EUR, 
pc.PRICE_USD JournalPrice_USD
from get_columns c
left join `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_gpu` gpu on c.PS_CODE = gpu.PS_CODE
left join `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_subject_collection` sc on c.JournalSubjectCollection = sc.CODE
left join `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jfw_prices_current_year` pc
on c.JournalKey = cast(pc.JOURNAL as STRING)