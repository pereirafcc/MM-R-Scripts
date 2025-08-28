#install.packages(c("DBI", "RODBC", "odbc", "dplyr", "dbplyr"))
#install.packages("RPostgres")
library(DBI)
library(RODBC)
library(data.table)
library(odbc)
library(dplyr)
library(dbplyr)
library(RPostgres)
library("salesforcer")
sf_auth()
today = Sys.Date()

con <- dbConnect(RPostgres::Postgres(),
                    dbname = "fcc",
                    host = "db-instance-0.cufbqcappuyq.us-east-1.rds.amazonaws.com",
                    user = "fccreadonly",
                    password = "rI5RC85nRjaabucO@^3%urgH@s6^74%TapKZ!wgs")

report <- dbGetQuery(con, "select crm_id, event_date, event_name, raw_ticket_id, area, paid, price_type from report.ticket t where is_comp_ticket = FALSE and event_name in ('250705CHI')")

#report <- dbGetQuery(con, "select crm_id, event_date, event_name, raw_ticket_id, area, paid, price_type from report.ticket")

report2 <- dbGetQuery(con, "select scanned_item_id, scan_succeeded from ringside.attendance a where scan_succeeded = true")

report3 <- left_join(report, report2, by = c('raw_ticket_id' = 'scanned_item_id'))
#report3 <- filter(report3, !is.na(scan_succeeded))


##SELECT EVENT NAME AND REPS YOU'RE LOOKING FOR##
report3 <- filter(report3, report3$event_name == '250705CHI')
reps <- data.frame("Name" = c('Undistributed Lead'))
report3 <- filter(report3, report3$price_type == 'Adult (Adult)')
report3 <- report3[!apply(report3, 1, function(row) any(grepl("Loge", row))),]

##Scrub STM/OBR##
query <- sf_query("SELECT Primary_Ticketing_ID__pc FROM Account where Primary_Ticketing_ID__pc not in ('') and Full_Season_Ticket_Holder__pc = true")
query2 <- sf_query("SELECT Primary_Ticketing_ID__pc FROM Account where Primary_Ticketing_ID__pc not in ('') and Partial_Season_Ticket_Holder__pc = true")
query3 <- sf_query("SELECT Primary_Ticketing_ID__pc FROM Account where Primary_Ticketing_ID__pc not in ('') and Waitlist_Priority_Rank__pc not in ('')")

query3 <- rbind(query, query2, query3)

report3 <- filter(report3, !(crm_id %in% query3$Primary_Ticketing_ID__pc))
report4 <- data.frame(report3$crm_id)
report4 <- distinct(report4)

expr_Id1 <- paste0("('", paste0(report4$report3.crm_id[1:nrow(report4)], collapse = "','"),"')")
#expr_Id2 <- paste0("('", paste0(report4$report3.crm_id[501:1000], collapse = "','"),"')")
#expr_Id3 <- paste0("('", paste0(report4$report3.crm_id[1001:1500], collapse = "','"),"')")
#expr_Id4 <- paste0("('", paste0(report4$report3.crm_id[1501:nrow(report4)], collapse = "','"),"')")


##SCRUB RECENTLY CONTACTED##
query411 <- sf_query(paste0("SELECT ID, Primary_Ticketing_ID__pc, Name, OwnerId, Days_Since_Last_Activity__c from Account where Primary_Ticketing_ID__pc in ",expr_Id1))
#query412 <- sf_query(paste0("SELECT ID, Primary_Ticketing_ID__pc, Name, OwnerId, Days_Since_Last_Activity__c from Account where Primary_Ticketing_ID__pc in ",expr_Id2))
#query413 <- sf_query(paste0("SELECT ID, Primary_Ticketing_ID__pc, Name, OwnerId, Days_Since_Last_Activity__c from Account where Primary_Ticketing_ID__pc in ",expr_Id3))
#query414 <- sf_query(paste0("SELECT ID, Primary_Ticketing_ID__pc, Name, OwnerId, Days_Since_Last_Activity__c from Account where Primary_Ticketing_ID__pc in ",expr_Id4))
query4 <- query411
#rbind(query411, query412, query413, query414)
query4$Primary_Ticketing_ID__pc <- as.integer(query4$Primary_Ticketing_ID__pc)
query12 <- left_join(query4, report3, by = c("Primary_Ticketing_ID__pc" = "crm_id"))
query4 <- query12[c("Id", "Days_Since_Last_Activity__c", "Name", "OwnerId", "Primary_Ticketing_ID__pc", "area", "paid")]
query5 <- filter(query4, is.na(query4$Days_Since_Last_Activity__c) | !(query4$Days_Since_Last_Activity__c < 30))

##SCRUB 2025 OPPORTUNITY##
query6 <- sf_query("SELECT AccountId, Start_Season_Name__c from Opportunity where Start_Season_Name__c in ('2025')")
query7 <- filter(query5, !(Id %in% query6$AccountId))

##SCRUB WAITLIST CAMPAIGN##
query60 <- sf_query("Select AccountId, CampaignId from Opportunity where CampaignId in ('701UP000009PurRYAS')")
query70 <- filter(query7, !(Id %in% query60$AccountId))

##SCRUB DO NOT CALL##
query8 <- sf_query("Select Id, PersonDoNotCall from Account where PersonDoNotCall = true")
query9 <-filter(query70, !(Id %in% query8$Id))

##SCRUB VISITING FAN##
query20 <- sf_query("Select Id, Visiting_Team_Fan__c from Account where Visiting_Team_Fan__c = true")
#query90
query11 <- filter(query9, !(Id %in% query20$Id))

##SCRUB SILENT PARTNER##
query21 <- sf_query("Select Id, Silent_Partner__c from Account where Silent_Partner__c = true")
#query90
query11 <- filter(query11, !(Id %in% query21$Id))

##SCRUB 2023/2024 Single Game Wins##
#query10 <- sf_query("Select AccountId, agilitek__Start_Season__c from Opportunity where Product__c in ('Single Game') and agilitek__Start_Season__c in ('a1d8Z000004gVxUQAU', 'a1d8Z000004gVxZQAU')")
#query11 <- filter(query90, !(Id %in% query10$AccountId))

##SCRUB 2025 HOME OPENER##
#report5 <- left_join(report, report2, by = c('raw_ticket_id' = 'ticket_id'))
#report5 <- filter(report5, report5$event_name == '250222NRB')
#query11 <- filter(query11, !(Primary_Ticketing_ID__pc %in% report5$crm_id))
#query11 <- distinct(query11)

##SCRUB MOVED OUT OF TOWN##
query30 <- sf_query("Select Id, Moved_Out_of_Town__c from Account where Moved_Out_of_Town__c = true")
query11 <- filter(query11, !(Id %in% query30$Id))
query32 <- sf_query("Select Id, Silent_Partner__c from Account where Silent_Partner__c = true")
query11 <- filter(query11, !(Id %in% query32$Id))

#SCRUB CONTACT ROLE##
query31 <- sf_query("SELECT Primary_Ticketing_ID__c FROM OpportunityContactRole")
query31 <- distinct(query31)
query11 <- filter(query11, !(Primary_Ticketing_ID__pc %in% query31$Primary_Ticketing_ID__c))


##PULL IN OWNERS WITH IDs##
Owners <- sf_query("Select Name, Id from User where IsActive = TRUE")



##CREATE OPPORTUNITIES##
#reps <- data.frame("Name" = c('Joe Coit'))
reps <- left_join(reps, Owners, by = c('Name' = 'Name'))
reps <-  cbind(reps, rep(row.names(reps), each = 100))
reps <- reps[1:(nrow(query11)),]

CreateOppswOwners <-data.frame("AccountId" = query11$Id, 
                               "Name" = "Ticketing Opp Upload", 
                               "agilitek__Start_Season__c" = "a1d8Z000004gVxWQAU",
                               "CloseDate" = paste0(today+30,"T00:00:00Z"),
                               "CampaignId" = "701UP00000JcGVTYA3",
                               "Follow_Up_By__c" = paste0(today, "T00:00:00Z"),
                               "NextStep" = "Initial Phone Call",
                               "StageName" = "No Contact",
                               "OwnerId" = reps$Id,
                               "Type" = "New",
                               "LeadSource" = "Previous Buyer",
                               "Number_of_Tickets__c" = 0,
                               "Amount" = 0,
                               "RecordTypeId" = "0128Z0000002EsaQAE",
                               "Follow_Up_Priority__c" = "b_Warm",
                               "Description" = paste0("Attended 7/5 Match vs CHI. Sat in ",query11$area,". Seats worth $",query11$paid)
                               )

CreateOppswOwners <- distinct(CreateOppswOwners)

##SCRUB PHONE NUMBER##
expr_Id<- paste0("('", paste0(CreateOppswOwners$AccountId[1:nrow(CreateOppswOwners)], collapse = "','"),"')")
Phone <- sf_query(paste0("Select Id, Phone from Account where Phone not in ('') and Id in ",expr_Id))
CreateOppswOwners <- filter(CreateOppswOwners, (AccountId %in% Phone$Id))

fwrite(CreateOppswOwners, file = '/Users/marino/Desktop/CLB.csv')
                               