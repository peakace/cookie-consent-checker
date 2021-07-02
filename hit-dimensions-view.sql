WITH base AS (
  SELECT 
        timestamp,
        REGEXP_EXTRACT(URI, "eventName=([^&]+)") as Event_Name,      
        REGEXP_EXTRACT(URI, "cookieValue=([^&]+)") as Cookie_Value,
        REGEXP_EXTRACT(URI, "consent=([^&]+)") as Click_Text,
        REGEXP_EXTRACT(URI, "consentStatus=([^&]+)") as Consent_Status,
        REGEXP_EXTRACT(URI, "url=([^&]+)") as Page_Path,
        REGEXP_EXTRACT(URI, "channel=([^&]+)") as Source_Medium,
        REGEXP_EXTRACT(URI, "bannerVersion=([^&]+)") as Banner_Version,
        REGEXP_EXTRACT(URI, "source_campaign=([^&]+)") as Campaign,
        REGEXP_EXTRACT(URI, "source_content=([^&]+)") as Content,
        REGEXP_EXTRACT(URI, "source_term=([^&]+)") as Term,
        REGEXP_EXTRACT(URI, "agent=([^&]+)") as User_Agent,
        REGEXP_EXTRACT(URI, "browser=([^&]+)") as Browser,
        REGEXP_EXTRACT(URI, "os=([^&]+)") as Operating_System,
        REGEXP_EXTRACT(URI, "osVersion=([^&]+)") as Operating_System_Version,
        REGEXP_EXTRACT(URI, "language=([^&]+)") as Language,
        REGEXP_EXTRACT(URI, "isBot=([^&]+)") as is_Bot,
        REGEXP_EXTRACT(URI, "botName=([^&]+)") as Bot_Name,
		REGEXP_EXTRACT(URI, "deviceType=([^&]+)") as Device_Type,
        DATE(timestamp) as Date
  FROM
       `{{here-your-project-id}}.cookie_consent_check.cookie_consent_request_table`
)
SELECT *, 
        CASE WHEN Event_Name IN ("bannerimpression") THEN 1 ELSE 0 END as bannerImpression,
        CASE WHEN Event_Name IN ("bannerclick") THEN 1 ELSE 0 END as bannerClick,        
FROM base