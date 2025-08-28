library(data.table)
sf_auth()

#PULL ALL JOE OPPS In 2024##
query <- sf_query("Select Id, AccountId, Product__c, CreatedDate, Description, Follow_Up_By__c, CloseDate from Opportunity where OwnerId in ('0058Z000009eIjdQAE') and CreatedDate > 2023-07-01T00:00:00Z and CampaignId in ('701UP000004MWOkYAO')")

#SCRUB ANY ACCOUNTS WITH OPEN 2025 OPP#
query8 <- sf_query("Select Id, AccountId, Product__c from Opportunity where OwnerId not in ('0058Z000009eIjdQAE') and Start_Season_Name__c in ('2025')")
query <- filter(query, !(query$AccountId %in% query8$AccountId))

#SCRUB ANY ACCOUNTS CONTACTED LAST 30#
expr_Id <- paste0("('", paste0(query$AccountId[1:nrow(query)], collapse = "','"),"')")
expr_Id2 <- paste0("('", paste0(query$AccountId[501:nrow(query)], collapse = "','"),"')")
query9 <- sf_query(paste0("Select Id, Ticketing_Service_Rep__pc, LastActivityDate, Last_Contacted_By__c from Account where Id in ", expr_Id))
query10 <- sf_query(paste0("Select Id, Ticketing_Service_Rep__pc, LastActivityDate, Last_Contacted_By__c from Account where Id in ", expr_Id2))
today = Sys.Date()
query19 <- rbind(query9, query10)
query9 <- filter(query9, query9$LastActivityDate > today - 30)
query <- filter(query, !(query$AccountId %in% query9$Id))

#SCRUB ANY STM/OBR#
query7 <- sf_query("SELECT Id FROM Account where Primary_Ticketing_ID__pc not in ('') and Full_Season_Ticket_Holder__pc = true")
query6 <- sf_query("SELECT Id FROM Account where Primary_Ticketing_ID__pc not in ('') and Partial_Season_Ticket_Holder__pc = true")
query5 <- sf_query("SELECT Id FROM Account where Primary_Ticketing_ID__pc not in ('') and Waitlist_Priority_Rank__pc not in ('')")
query <- filter(query, !(query$AccountId %in% query7$Id))
query <- filter(query, !(query$AccountId %in% query6$Id))
query <- filter(query, !(query$AccountId %in% query5$Id))

#SCRUB 2024 WINS#
query4 <- sf_query("Select AccountId from Opportunity where Start_Season_Name__c in ('2024') and StageName in ('Closed Won')")
query <- filter(query, !(query$AccountId %in% query4$AccountId))

#SCRUB 2024 MARKETING TBITS NON-JOE#
query3 <- sf_query("Select Id, AccountId, Product__c from Opportunity where OwnerId not in ('0058Z000009eIjdQAE') and CampaignId in ('701UP000004MWOkYAO')")
query <- filter(query, !(query$AccountId %in% query3$AccountId))

query$CloseDate <- today+30
query$Follow_Up_By__c <- today+14


#FILTER BY PRODUCT#
waitlist <- filter(query, query$Product__c == ('Waitlist'))
single <- filter(query, query$Product__c == ('Single Game'))
groups <- filter(query, query$Product__c == ('Group'))
premium <- filter(query, query$Product__c == ('Premium Single Deposit'))
blank <- filter(query, is.na(query$Product__c))

fwrite(single, file = '/Users/marino/Desktop/marketingDistribution.csv')


