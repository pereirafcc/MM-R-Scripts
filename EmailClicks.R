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


report <- dbGetQuery(con, "
select email, count(distinct event_name) as games
from custom.churn
where event_date > '2023-02-14 19:30:00.000000'
group by email")


report2 <- dbGetQuery(con, "select distinct emailaddress
                      from sfmc.clicks
                      where sendid in ('42358121')")

#report3 <- filter(report, (email %in% report2$emailaddress))
#report4 <- filter(report3, (report3$games >= 1))
report4 <- report2
report4$email <- report4$emailaddress
                      
                      
report5 <- dbGetQuery(con, "
                        select distinct email
                        from custom.ticket_pacing_and_inventory
                        where event_name = '241109NYC'
                        AND email IS NOT NULL"
                      )

report6 <- filter(report4, !(email %in% report5$email))

report7 <- sf_query("Select AccountId from Opportunity where CampaignId in ('701UP00000F2S2tYAF')")
expr_Id<- paste0("('", paste0(report7$AccountId[1:nrow(report7)], collapse = "','"),"')")
#expr_Id2<- paste0("('", paste0(report7$AccountId[401:nrow(report7)], collapse = "','"),"')")
query0 <- sf_query(paste0("Select PersonEmail from Account where Id in", expr_Id))
#query00 <- sf_query(paste0("Select PersonEmail from Account where Id in", expr_Id2))
#query0 <-rbind(query0, query00)
query0$PersonEmail <- tolower(query0$PersonEmail)
report6$email <- tolower(report6$email)
report6 <- filter(report6, !email %in% query0$PersonEmail)

report8 <- sf_query("SELECT PersonEmail FROM Account where PersonMailingState in ('OH', 'Ohio', 'KY', 'Kentucky') and PersonEmail not in ('')")
report6 <- filter(report6, email %in% report8$PersonEmail)

expr_Id<- paste0("('", paste0(report6$email[1:nrow(report6)], collapse = "','"),"')")


##################

query1 <- sf_query(paste0("Select Id, PersonEmail, Last_Contacted_By__c, Days_Since_Last_Activity__c, Full_Season_Ticket_Holder__pc, Waitlist_Priority_Rank__pc, Ticketing_Sales_Rep__c, Ticketing_Service_Rep__pc, Phone from Account where PersonEmail in", expr_Id))
query2 = query1 %>% filter(duplicated(PersonEmail) == FALSE)
query2 = query2 %>% filter(!is.na(Phone))
Owners <- sf_query("Select Name, Id from User where IsActive = TRUE")

##ADD REP TO STMS##
stms <- filter(query2, (query2$Full_Season_Ticket_Holder__pc == TRUE))
stms$OwnerId <- ifelse(!(is.na(stms$Ticketing_Service_Rep__pc)), stms$Ticketing_Service_Rep__pc, stms$Last_Contacted_By__c)
stms <- left_join(stms, Owners, by = c("OwnerId" = "Id"))
stmsfinal <- select(stms, c('Id', 'OwnerId', 'Name'))
stmsfinal$OwnerId <- ifelse(!(is.na(stmsfinal$Name)), stmsfinal$OwnerId, '0051U000006YPn1QAG')
stmsfinal$Name <- ifelse(!(is.na(stmsfinal$Name)), stmsfinal$Name, 'Lukas Brant')


##ADD REP TO OBR##
query3 <- filter(query2, !(PersonEmail %in% stms$PersonEmail))
obr <- filter(query3, !is.na(query3$Waitlist_Priority_Rank__pc))
obr$OwnerId <- ifelse(!(is.na(obr$Ticketing_Service_Rep__pc)), obr$Ticketing_Service_Rep__pc, obr$Last_Contacted_By__c)
obr <- left_join(obr, Owners, by = c("OwnerId" = "Id"))
obrfinal <- select(obr, c('Id', 'OwnerId', 'Name'))
obrfinal$OwnerId <- ifelse(!(is.na(obrfinal$Name)), obrfinal$OwnerId, '0051U000006YPn1QAG')
obrfinal$Name <- ifelse(!(is.na(obrfinal$Name)), obrfinal$Name, 'Lukas Brant')


##ADD REP TO Last Contacted##
query4 <- filter(query3, !(PersonEmail %in% obr$PersonEmail))
recentcontact <- filter(query4, (query4$Days_Since_Last_Activity__c <= 30))
recentcontact$OwnerId <- recentcontact$Last_Contacted_By__c
recentcontact <- left_join(recentcontact, Owners, by = c("OwnerId" = "Id"))
recentcontactfinal <- select(recentcontact, c('Id', 'OwnerId', 'Name'))


##DISTRIBUTE THE REST##
query5 <- filter(query4, !PersonEmail %in% recentcontact$PersonEmail)
reps <- data.frame("Name" = c('Zack Lutz', 'Cassie Collins', 'Ian McCarthy', 'Shawn Mather', 'Olivia Craddock'))
reps <- left_join(reps, Owners, by = c('Name' = 'Name'))
reps <-  cbind(reps, rep(row.names(reps), each = 20))
reps <- reps[1:(nrow(query5)),]
roundrobin <- data.frame(query5$Id, reps$Id, reps$Name)
colnames(roundrobin) <- c("Id", "OwnerId", "Name")


###COMBINE ALL 4###
combined <- rbind(stmsfinal, obrfinal, recentcontactfinal, roundrobin)
reps1 <- data.frame(
  name = c('Zack Lutz', 'Cassie Collins', 'Ian McCarthy', 'Shawn Mather', 'Olivia Craddock'))
combined <- combined %>% filter(Name %in% reps1$name)


CreateOppswOwners <-data.frame("AccountId" = combined$Id,
                               "Name" = "Ticketing Opp Upload",
                               "agilitek__Start_Season__c" = "a1d8Z000004gVxZQAU",
                               "CloseDate" = paste0(today+30,"T00:00:00Z"),
                               "CampaignId" = "701UP00000F2S2tYAF",
                               "Follow_Up_By__c" = paste0(today, "T00:00:00Z"),
                               "NextStep" = "Initial Phone Call",
                               "StageName" = "New",
                               "OwnerId" = combined$OwnerId,
                               "Type" = "New",
                               "Product__c" = "Single Game",
                               "LeadSource" = "Marketing",
                               "Number_of_Tickets__c" = 0,
                               "Amount" = 0,
                               "RecordTypeId" = "0128Z0000002EsaQAE",
                               "Follow_Up_Priority__c" = "a_hot",
                               "Description" = "Game 3 email click. Has attended at least 1 match in the last 2 years."
)


CreateOppswOwners <- distinct(CreateOppswOwners)

fwrite(CreateOppswOwners, file = '/Users/marino/Desktop/FEEFREE.csv')



