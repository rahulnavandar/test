CREATE OR REPLACE TABLE `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Material_Base_Reporting_Monthly` (
UsageCalenderDate DATE, 
UsageCalenderYear INT64, 
UsageCalenderMonth INT64, 
UsageCalenderYearMonth STRING, 
UsageCalenderQuarter INT64, 
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
UsageRobot STRING, 
UsageFullBookYN STRING, 
UsageClicks INT64, 
UsageWholeBookDownload INT64,
PMkey STRING)
 PARTITION BY UsageCalenderDate
 CLUSTER BY UsageItemDOI
AS
SELECT DATE_TRUNC(UsageCalenderDate,MONTH) UsageCalenderDate, UsageCalenderYear, UsageCalenderMonth, UsageCalenderYearMonth, UsageCalenderQuarter, UsageAuthenticationType, UsageAuthenticationValue, UsagePlatform, UsagePublisherName, UsagePublisherType, UsageBookJournalDOI, UsagePublicationYear, UsageItemDOI, UsageContentType, UsagePrintISSN, UsageElectronicISSN, UsagePrintISBN, UsageElectronicISBN, UsageSeriesISSN_E, UsageSeriesISSN_P, UsageReturnCode, UsageAccessYN, UsageFederatedSearch, UsageOpenAccessYN, UsageJournalNo, UsageRobot, UsageFullBookYN, sum(UsageClicks) UsageClicks, sum(UsageWholeBookDownload) UsageWholeBookDownload,PMkey FROM `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Material_Base_Reporting` 
GROUP BY 
DATE_TRUNC(UsageCalenderDate,MONTH), UsageCalenderYear, UsageCalenderMonth, UsageCalenderYearMonth, UsageCalenderQuarter, UsageAuthenticationType, UsageAuthenticationValue, UsagePlatform, UsagePublisherName, UsagePublisherType, UsageBookJournalDOI, UsagePublicationYear, UsageItemDOI, UsageContentType, UsagePrintISSN, UsageElectronicISSN, UsagePrintISBN, UsageElectronicISBN, UsageSeriesISSN_E, UsageSeriesISSN_P, UsageReturnCode, UsageAccessYN, UsageFederatedSearch, UsageOpenAccessYN, UsageJournalNo, UsageRobot, UsageFullBookYN,PMkey