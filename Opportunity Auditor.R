library(data.table)
sf_auth()

con <- dbConnect(RPostgres::Postgres(),
                 dbname = "fcc",
                 host = "db-instance-0.cufbqcappuyq.us-east-1.rds.amazonaws.com",
                 user = "fccreadonly",
                 password = "rI5RC85nRjaabucO@^3%urgH@s6^74%TapKZ!wgs")

#SELECT CLOSED WON OPPS SINCE 7/25##
query <- sf_query("Select Id, AccountId, LeadSource, Name, Completed_Tasks__c, Start_Season_Name__c, CreatedById, Primary_Campaign_Source_ListView__c, Product__c, Number_of_Tickets__c, Amount from Opportunity where StageName in ('Closed Won') and Type not in ('Maintenance') and CloseDate > 2024-08-01 and Audited__c = false")

#ENSURE COMPLETED TASK#
failedquery1 <- filter(query, is.na(query$Completed_Tasks__c))

##ENSURE START SEASON MATCHES NAME##
failedquery2 <- query
failedquery2$conc <- substr(failedquery2$Start_Season_Name__c, start = 1, stop = 4)
failedquery2 <- filter(failedquery2, !(Start_Season_Name__c == conc))

##ENSURE THAT THEY DIDN'T CREATE A WEB LEAD##
failedquery3 <- filter(query, LeadSource == 'Web Interest Form')
failedquery3 <- filter(failedquery3, !(CreatedById == ('0058Z000009eIjdQAE')))
failedquery3 <- filter(failedquery3, !(CreatedById == ('0058Z000007ysMAQAY')))
failedquery3 <- filter(failedquery3, !(CreatedById == ('005UP0000063O7CYAU')))

##ENSURE RECORDTYPE ID ON BUSINESS OPPORTUNITY##
failedquery5 <- sf_query("Select Id from Account where RecordTypeId in ('0128Z0000002EgoQAE')")
failedquery4 <- filter(query, query$AccountId %in% failedquery5$Id)
report <- dbGetQuery(con, "select distinct opportunityid from sfdc.opportunitycontactrole")
failedquery4 <- filter(failedquery4, !(Id %in% report$opportunityid))

##ADD AUDITED FIELD##
failedquery10 <- data.frame(failedquery1$Id)
failedquery20 <- data.frame(failedquery2$Id)
failedquery30 <- data.frame(failedquery3$Id)
failedquery40 <- data.frame(failedquery4$Id)
failedquery <- data.frame(rbindlist(list(failedquery10, failedquery20, failedquery30, failedquery40)))
failedquery <- failedquery %>% rename(Id = failedquery1.Id)

query$Audited__c <- ifelse(query$Id %in% failedquery$Id, FALSE, TRUE)
query <- query[c("Id", "Product__c", "Number_of_Tickets__c", "Amount", "Audited__c")]
query$ReasonFailed <- ifelse(query$Id %in% failedquery10$failedquery1.Id, "No Tasks", ifelse(query$Id %in% failedquery20$failedquery3.Id, "Season Does Not Match Name", ifelse(query$Id %in% failedquery30$failedquery3.Id, "Created Web Lead",ifelse(query$Id %in% failedquery40$failedquery4.Id, "No Related Contacts",NA))))

fwrite(query, file = '/Users/marino/Desktop/Audit.csv')

