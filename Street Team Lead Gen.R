#install.packages(c("DBI", "RODBC", "odbc", "dplyr", "dbplyr"))
#install.packages("RPostgres")
library(DBI)
library(RODBC)
library(data.table)
library(dplyr)
library(dbplyr)
library(RPostgres)
library("salesforcer")
sf_auth()
today = Sys.Date()

##LAST PULL DATE: XX/XX/2025
lastpull <- as.Date("07/24/2025", format = "%m/%d/%Y")


con <- dbConnect(RPostgres::Postgres(),
                 dbname = "fcc",
                 host = "db-instance-0.cufbqcappuyq.us-east-1.rds.amazonaws.com",
                 user = "fccreadonly",
                 password = "rI5RC85nRjaabucO@^3%urgH@s6^74%TapKZ!wgs")


report <- dbGetQuery(con, "SELECT DISTINCT ON (sfdc.recordid)
LEFT(ca.campaign_title::text, 80) AS campaign_title_short,
ca.campaign_title,
ca.creation_timestamp_iso as date_submitted,
sfdc.recordid AS sfdc_accountid,
MAX(
  CASE
  WHEN cad.field_name::text = 'cr_how_did_you_hear' OR
  cad.field_name::text = 'Where did you meet us?'
  THEN cad.field_value
  END
) AS event,
MAX(
  CASE
  WHEN cad.field_name::text = 'ticket_interest'
  THEN cad.field_value
  END
) AS ticket_interest
FROM tradable_bits.crm_activity ca
JOIN tradable_bits.crm_fans cf
ON ca.fan_id = cf.fan_id
JOIN tradable_bits.crm_activities_detail cad
ON ca.activity_id = cad.activity_id
JOIN fdp.email f
ON LOWER(ca.email::text) = LOWER(f.email)
JOIN fdp.identity sfdc
ON sfdc.systemname = 'sfdc' AND sfdc.fanid = f.fanid
WHERE ca.campaign_title = 'Community Relations / Street Team & Community Outreach Link Tree 2025'
AND EXISTS (
  SELECT 1
  FROM tradable_bits.crm_activities_detail cad2
  WHERE cad2.activity_id = ca.activity_id
  AND cad2.field_name = 'ticket_interest'
  AND cad2.field_value = 'checked'
)
GROUP BY
LEFT(ca.campaign_title::text, 80),
ca.campaign_title,
CONCAT_WS('-'::text, sfdc.recordid, ca.creation_timestamp_iso),
sfdc.recordid,
cf.add_phone,
ca.creation_timestamp_iso
ORDER BY sfdc.recordid DESC;")

report$date_submitted <- as.Date(report$date_submitted)
PullActiveOpps <- sf_query("SELECT Id, AccountId, StageName FROM Opportunity where CampaignId in ('701UP00000J8szbYAB') and StageName not in ('Closed Won', 'Closed Lost')")
report2 <- report %>% filter(!report$sfdc_accountid %in% PullActiveOpps$AccountId)
report3 <- report2 %>% filter(report2$date_submitted > lastpull)

##Scrub STM/OBR##
query <- sf_query("SELECT Id FROM Account where Primary_Ticketing_ID__pc not in ('') and Full_Season_Ticket_Holder__pc = true")
query2 <- sf_query("SELECT Id FROM Account where Primary_Ticketing_ID__pc not in ('') and Partial_Season_Ticket_Holder__pc = true")
query3 <- sf_query("SELECT Id FROM Account where Primary_Ticketing_ID__pc not in ('') and Waitlist_Priority_Rank__pc not in ('')")

query3 <- rbind(query, query2, query3)

report3 <- report3 %>% filter(!report3$sfdc_accountid %in% query3$Id)


CreateOppswOwners <-data.frame("AccountId" = report3$sfdc_accountid, 
                               "Name" = "Opportunity Upload", 
                               "agilitek__Start_Season__c" = "a1d8Z000004gVxWQAU",
                               "CloseDate" = paste0(today+15,"T00:00:00Z"),
                               "CampaignId" = "701UP00000J8szbYAB",
                               "Follow_Up_By__c" = paste0(today, "T00:00:00Z"),
                               "NextStep" = "Initial Phone Call",
                               "StageName" = "No Contact",
                               "OwnerId" = "005UP000009TFCzYAO",
                               "Type" = "New",
                               "LeadSource" = "FCC Sponsored Events",
                               "Number_of_Tickets__c" = 0,
                               "Amount" = 0,
                               "RecordTypeId" = "0128Z0000002EsaQAE",
                               "Follow_Up_Priority__c" = "c_cold",
                               "Description" = paste0("Attended ",report3$event)
)


result <- sf_create(CreateOppswOwners, object_name = "Opportunity")








