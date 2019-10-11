CREATE OR REPLACE TABLE `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_BP_Base_Reporting_MonthlyV2` (
PMKey STRING,
UsageCalenderDate DATE, 
UsageCalenderYear INT64,
UsageCalenderMonth INT64,
UsageCalenderYearMonth STRING,
UsageCalenderQuarter INT64,
UsageBusinessPartner STRING,
UsageBusinessPartnerType STRING,
UsageAuthenticationType STRING,
UsageAuthenticationValue STRING,
UsagePlatform STRING,
UsagePublisherName STRING,
UsagePublisherType STRING,
UsageBookJournalDOI STRING,
UsagePublicationYear INT64,
UsageItemDOI STRING,
UsageContentType STRING,
UsagePrintISSN STRING,
UsageElectronicISSN STRING,
UsagePrintISBN STRING,
UsageElectronicISBN STRING, 
UsageSeriesISSN_E STRING,
UsageSeriesISSN_P STRING,
UsageReturnCode STRING,
UsageAccessYN STRING,
UsageFederatedSearch STRING,
UsageOpenAccessYN STRING,
UsageJournalNo STRING,
UsageFullBookYN STRING,
UsageClicks INT64
)
 PARTITION BY UsageCalenderDate
 CLUSTER BY UsagePublisherType, UsageBusinessPartner, UsageItemDOI
AS
SELECT PMKey, DATE_TRUNC(UsageCalenderDate, MONTH) UsageCalenderDate, UsageCalenderYear, UsageCalenderMonth, UsageCalenderYearMonth, UsageCalenderQuarter, UsageBusinessPartner, UsageBusinessPartnerType, UsageAuthenticationType, UsageAuthenticationValue, UsagePlatform, UsagePublisherName, UsagePublisherType, UsageBookJournalDOI, UsagePublicationYear, UsageItemDOI, UsageContentType, UsagePrintISSN, UsageElectronicISSN, UsagePrintISBN, UsageElectronicISBN, UsageSeriesISSN_E, UsageSeriesISSN_P, UsageReturnCode, UsageAccessYN, UsageFederatedSearch, UsageOpenAccessYN, UsageJournalNo, UsageFullBookYN, sum(UsageClicks) UsageClicks FROM `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_BP_Base_Reporting`
GROUP BY Pmkey,DATE_TRUNC(UsageCalenderDate, MONTH), UsageCalenderYear, UsageCalenderMonth, UsageCalenderYearMonth, UsageCalenderQuarter, UsageBusinessPartner, UsageBusinessPartnerType, UsageAuthenticationType, UsageAuthenticationValue, UsagePlatform, UsagePublisherName, UsagePublisherType, UsageBookJournalDOI, UsagePublicationYear, UsageItemDOI, UsageContentType, UsagePrintISSN, UsageElectronicISSN, UsagePrintISBN, UsageElectronicISBN, UsageSeriesISSN_E, UsageSeriesISSN_P, UsageReturnCode, UsageAccessYN, UsageFederatedSearch, UsageOpenAccessYN, UsageJournalNo, UsageFullBookYN