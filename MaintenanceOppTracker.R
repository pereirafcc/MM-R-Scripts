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

#PULL IN ALL MAINTENANCE OPPORTUNITIES#
query <- sf_query("Select Id, Account_Name_Slack__c, AccountId, Product__c, OwnerId from Opportunity where Type in ('Maintenance') and agilitek__Start_Season__c in ('a1d8Z000004gVxWQAU') and StageName not in ('Closed Won', 'Closed Lost')")
Owners <- sf_query("Select Name, Id from User where IsActive = TRUE")
query <- left_join(query, Owners, by=c("OwnerId"="Id"))
AccountIds <- data.frame(query$AccountId)
AccountIds <- na.omit(AccountIds)
expr_Id<- paste0("('", paste0(AccountIds$query.AccountId[1:500], collapse = "','"),"')")
expr_Id2<- paste0("('", paste0(AccountIds$query.AccountId[501:1000], collapse = "','"),"')")
expr_Id3<- paste0("('", paste0(AccountIds$query.AccountId[1001:1500], collapse = "','"),"')")
expr_Id4<- paste0("('", paste0(AccountIds$query.AccountId[1501:2000], collapse = "','"),"')")
expr_Id5<- paste0("('", paste0(AccountIds$query.AccountId[2001:2500], collapse = "','"),"')")
expr_Id6<- paste0("('", paste0(AccountIds$query.AccountId[2501:3000], collapse = "','"),"')")
expr_Id7<- paste0("('", paste0(AccountIds$query.AccountId[3001:3500], collapse = "','"),"')")
expr_Id8<- paste0("('", paste0(AccountIds$query.AccountId[3501:4000], collapse = "','"),"')")
expr_Id9<- paste0("('", paste0(AccountIds$query.AccountId[4001:4500], collapse = "','"),"')")
expr_Id10<- paste0("('", paste0(AccountIds$query.AccountId[4501:5000], collapse = "','"),"')")
expr_Id11<- paste0("('", paste0(AccountIds$query.AccountId[5001:5500], collapse = "','"),"')")
expr_Id12<- paste0("('", paste0(AccountIds$query.AccountId[5501:6000], collapse = "','"),"')")
expr_Id13<- paste0("('", paste0(AccountIds$query.AccountId[6001:6500], collapse = "','"),"')")
expr_Id14<- paste0("('", paste0(AccountIds$query.AccountId[6501:7000], collapse = "','"),"')")
expr_Id15<- paste0("('", paste0(AccountIds$query.AccountId[7001:nrow(AccountIds)], collapse = "','"),"')")
BusinessPersonal1 <- sf_query(paste0("Select Name, Id, Contact_ID__pc, Primary_Ticketing_ID__pc, RecordTypeId from Account where Id in ",expr_Id))
BusinessPersonal2 <- sf_query(paste0("Select Name, Id, Contact_ID__pc, Primary_Ticketing_ID__pc, RecordTypeId from Account where Id in ",expr_Id2))
BusinessPersonal3 <- sf_query(paste0("Select Name, Id, Contact_ID__pc, Primary_Ticketing_ID__pc, RecordTypeId from Account where Id in ",expr_Id3))
BusinessPersonal4 <- sf_query(paste0("Select Name, Id, Contact_ID__pc, Primary_Ticketing_ID__pc, RecordTypeId from Account where Id in ",expr_Id4))
BusinessPersonal5 <- sf_query(paste0("Select Name, Id, Contact_ID__pc, Primary_Ticketing_ID__pc, RecordTypeId from Account where Id in ",expr_Id5))
BusinessPersonal6 <- sf_query(paste0("Select Name, Id, Contact_ID__pc, Primary_Ticketing_ID__pc, RecordTypeId from Account where Id in ",expr_Id6))
BusinessPersonal7 <- sf_query(paste0("Select Name, Id, Contact_ID__pc, Primary_Ticketing_ID__pc, RecordTypeId from Account where Id in ",expr_Id7))
BusinessPersonal8 <- sf_query(paste0("Select Name, Id, Contact_ID__pc, Primary_Ticketing_ID__pc, RecordTypeId from Account where Id in ",expr_Id8))
BusinessPersonal9 <- sf_query(paste0("Select Name, Id, Contact_ID__pc, Primary_Ticketing_ID__pc, RecordTypeId from Account where Id in ",expr_Id9))
BusinessPersonal10 <- sf_query(paste0("Select Name, Id, Contact_ID__pc, Primary_Ticketing_ID__pc, RecordTypeId from Account where Id in ",expr_Id10))
BusinessPersonal11 <- sf_query(paste0("Select Name, Id, Contact_ID__pc, Primary_Ticketing_ID__pc, RecordTypeId from Account where Id in ",expr_Id11))
BusinessPersonal12 <- sf_query(paste0("Select Name, Id, Contact_ID__pc, Primary_Ticketing_ID__pc,RecordTypeId from Account where Id in ",expr_Id12))
BusinessPersonal13 <- sf_query(paste0("Select Name, Id, Contact_ID__pc, Primary_Ticketing_ID__pc,RecordTypeId from Account where Id in ",expr_Id13))
BusinessPersonal14 <- sf_query(paste0("Select Name, Id, Contact_ID__pc, Primary_Ticketing_ID__pc,RecordTypeId from Account where Id in ",expr_Id14))
BusinessPersonal15 <- sf_query(paste0("Select Name, Id, Contact_ID__pc, Primary_Ticketing_ID__pc,RecordTypeId from Account where Id in ",expr_Id15))
BusinessPersonal <- rbind(BusinessPersonal1, BusinessPersonal2, BusinessPersonal3, BusinessPersonal4, BusinessPersonal5, BusinessPersonal6, BusinessPersonal7, BusinessPersonal8, BusinessPersonal9, BusinessPersonal10, BusinessPersonal11, BusinessPersonal12, BusinessPersonal13, BusinessPersonal14, BusinessPersonal15)
RecordType <- sf_query("Select Id, Name from RecordType")
BusinessPersonal <- left_join(BusinessPersonal, RecordType, by=c("RecordTypeId"="Id"))
colnames(BusinessPersonal) <- c("AccountId", "ContactId", "AccountName", "TicketingId", "RecordTypeId", "RecordType")
query <- left_join(query,BusinessPersonal, by=c("AccountId"="AccountId"))
colnames(query) <- c("Id", "AccountName", "AccountId", "OwnerId", "Product", "OwnerName", "ContactId", "Account Name", "TicketingId", "RecordTypeId", "RecordType")

query <- query[c("Id", "AccountName", "AccountId", "Product","ContactId", "TicketingId", "OwnerId", "OwnerName", "RecordType")]

BusinessAccounts <- filter(query, RecordType =='Business Account')
PersonAccounts <- filter (query, RecordType =='Person Account')

expr_Id100<- paste0("('", paste0(BusinessAccounts$Id[1:nrow(BusinessAccounts)], collapse = "','"),"')")
ContactRoleQuery <- sf_query(paste0("SELECT ContactId, OpportunityId FROM OpportunityContactRole where OpportunityId in ",expr_Id100))
BusinessAccounts <- left_join(BusinessAccounts, ContactRoleQuery, by=c("Id"="OpportunityId"))
BusinessAccounts <- BusinessAccounts[c("Id", "AccountName", "AccountId", "Product", "ContactId.y","TicketingId", "OwnerId", "OwnerName", "RecordType")]
colnames(BusinessAccounts) <- c("Id", "AccountName", "AccountId", "Product", "ContactId", "TicketingId", "OwnerId", "OwnerName", "RecordType")

AllAccounts <- rbind(PersonAccounts, BusinessAccounts)



##PULL JANUARY TASKS AND QUESTIONS##
WhatIdJanuaryTasks <- sf_query("SELECT WhatId FROM Task where Date_Completed__c > 2024-12-31 and Date_Completed__c < 2025-02-01 and WhatId not in ('')")
WhoIdJanuaryTasks <- sf_query("SELECT WhoId FROM Task where Date_Completed__c > 2024-12-31 and Date_Completed__c < 2025-02-01 and WhoId not in ('')")
WhatIdJanuaryTasks <- distinct(WhatIdJanuaryTasks)
WhoIdJanuaryTasks <- distinct(WhoIdJanuaryTasks)
WhoIdJanuaryTasks$ContactedAccount <- 1
WhatIdJanuaryTasks$ContactedOpp <- 1
WhoIdJanuaryTasks <- filter(WhoIdJanuaryTasks, (WhoId %in% AllAccounts$ContactId))
WhatIdJanuaryTasks <- filter(WhatIdJanuaryTasks, (WhatId %in% AllAccounts$Id))
AllAccounts <- left_join(AllAccounts, WhoIdJanuaryTasks, by=c("ContactId"="WhoId"))
AllAccounts <- left_join(AllAccounts, WhatIdJanuaryTasks, by=c("Id"="WhatId"))
AllAccounts$ContactedAccount[is.na(AllAccounts$ContactedAccount)] <- 0
AllAccounts$ContactedOpp[is.na(AllAccounts$ContactedOpp)] <- 0
AllAccounts$JanuaryContacted <- AllAccounts$ContactedAccount + AllAccounts$ContactedOpp
AllAccounts$JanuaryContacted <- ifelse(AllAccounts$JanuaryContacted>0,1,0)

##JANUARY TASKS ON ACCOUNTS RELATED TO BUSINESS ACCOUNTS##
expr_Id300 <- paste0("('", paste0(BusinessAccounts$Id[1:nrow(BusinessAccounts)], collapse = "','"),"')")
BusinessContacts <- sf_query(paste0("Select OpportunityId, ContactId from OpportunityContactRole where OpportunityId in ",expr_Id300))
expr_Id3000 <- paste0("('", paste0(BusinessContacts$ContactId[1:nrow(BusinessContacts)], collapse = "','"),"')")
BusinessContactAccounts <- sf_query(paste0("Select Id, Contact_ID__pc from Account where Contact_ID__pc in ",expr_Id3000))
BusinessContactAccounts <- left_join(BusinessContactAccounts, BusinessContacts, by = c("Contact_ID__pc"="ContactId"))
BusinessJanuaryTasks <- sf_query(paste0("Select WhoId from Task where Date_Completed__c > 2024-12-31 and Date_Completed__c < 2025-02-01 and WhoId in",expr_Id3000))
BusinessJanuaryTasks <- distinct(BusinessJanuaryTasks)
BusinessJanuaryTasks <- left_join(BusinessJanuaryTasks, BusinessContactAccounts, by = c("WhoId" = "Contact_ID__pc"))
AllAccounts$JanuaryContacted2 <- ifelse(AllAccounts$ContactId %in% BusinessJanuaryTasks$WhoId,1,0)
AllAccounts$JanuaryContacted <- AllAccounts$JanuaryContacted + AllAccounts$JanuaryContacted2
AllAccounts$JanuaryContacted <- ifelse(AllAccounts$JanuaryContacted>0,1,0)
AllAccounts <- subset(AllAccounts, select = -JanuaryContacted2)


##CHECK FOR PRIMARY COMPANY FIELD, THEN CHECK FOR RELATED ACCOUNT##
expr_Id200<- paste0("('", paste0(PersonAccounts$AccountId[1:500], collapse = "','"),"')")
expr_Id201<- paste0("('", paste0(PersonAccounts$AccountId[501:1000], collapse = "','"),"')")
expr_Id202<- paste0("('", paste0(PersonAccounts$AccountId[1001:1500], collapse = "','"),"')")
expr_Id203<- paste0("('", paste0(PersonAccounts$AccountId[1501:2000], collapse = "','"),"')")
expr_Id204<- paste0("('", paste0(PersonAccounts$AccountId[2001:2500], collapse = "','"),"')")
expr_Id205<- paste0("('", paste0(PersonAccounts$AccountId[2501:3000], collapse = "','"),"')")
expr_Id206<- paste0("('", paste0(PersonAccounts$AccountId[3001:3500], collapse = "','"),"')")
expr_Id207<- paste0("('", paste0(PersonAccounts$AccountId[3501:4000], collapse = "','"),"')")
expr_Id208<- paste0("('", paste0(PersonAccounts$AccountId[4001:4500], collapse = "','"),"')")
expr_Id209<- paste0("('", paste0(PersonAccounts$AccountId[4501:5000], collapse = "','"),"')")
expr_Id210<- paste0("('", paste0(PersonAccounts$AccountId[5001:5500], collapse = "','"),"')")
expr_Id211<- paste0("('", paste0(PersonAccounts$AccountId[5501:6000], collapse = "','"),"')")
expr_Id212<- paste0("('", paste0(PersonAccounts$AccountId[6001:6500], collapse = "','"),"')")
expr_Id213<- paste0("('", paste0(PersonAccounts$AccountId[6501:nrow(PersonAccounts)], collapse = "','"),"')")
PrimaryCompany0 <- sf_query(paste0("Select Id, Primary_Company__pc from Account where Primary_Company__pc not in ('') and Id in ",expr_Id200))
PrimaryCompany1 <- sf_query(paste0("Select Id, Primary_Company__pc from Account where Primary_Company__pc not in ('') and  Id in ",expr_Id201))
PrimaryCompany2 <- sf_query(paste0("Select Id, Primary_Company__pc from Account where Primary_Company__pc not in ('') and  Id in ",expr_Id202))
PrimaryCompany3 <- sf_query(paste0("Select Id, Primary_Company__pc from Account where Primary_Company__pc not in ('') and  Id in ",expr_Id203))
PrimaryCompany4 <- sf_query(paste0("Select Id, Primary_Company__pc from Account where Primary_Company__pc not in ('') and  Id in ",expr_Id204))
PrimaryCompany5 <- sf_query(paste0("Select Id, Primary_Company__pc from Account where Primary_Company__pc not in ('') and  Id in ",expr_Id205))
PrimaryCompany6 <- sf_query(paste0("Select Id, Primary_Company__pc from Account where Primary_Company__pc not in ('') and  Id in ",expr_Id206))
PrimaryCompany7 <- sf_query(paste0("Select Id, Primary_Company__pc from Account where Primary_Company__pc not in ('') and  Id in ",expr_Id207))
PrimaryCompany8 <- sf_query(paste0("Select Id, Primary_Company__pc from Account where Primary_Company__pc not in ('') and  Id in ",expr_Id208))
PrimaryCompany9 <- sf_query(paste0("Select Id, Primary_Company__pc from Account where Primary_Company__pc not in ('') and  Id in ",expr_Id209))
PrimaryCompany10 <- sf_query(paste0("Select Id, Primary_Company__pc from Account where Primary_Company__pc not in ('') and  Id in ",expr_Id210))
PrimaryCompany11 <- sf_query(paste0("Select Id, Primary_Company__pc from Account where Primary_Company__pc not in ('') and  Id in ",expr_Id211))
PrimaryCompany12 <- sf_query(paste0("Select Id, Primary_Company__pc from Account where Primary_Company__pc not in ('') and  Id in ",expr_Id212))
PrimaryCompany13 <- sf_query(paste0("Select Id, Primary_Company__pc from Account where Primary_Company__pc not in ('') and  Id in ",expr_Id213))
PrimaryCompany <- rbind(PrimaryCompany0, PrimaryCompany1, PrimaryCompany2, PrimaryCompany3, PrimaryCompany4, PrimaryCompany5, PrimaryCompany6, PrimaryCompany7, PrimaryCompany8, PrimaryCompany9, PrimaryCompany10, PrimaryCompany11, PrimaryCompany12, PrimaryCompany13)
PrimaryCompany <- left_join(PrimaryCompany, AllAccounts, by=c("Id"="AccountId"))
PrimaryCompany <- PrimaryCompany[c("Primary_Company__pc", "ContactId")]

AccountContactRelation <- sf_query("Select AccountId, ContactId from AccountContactRelation")
colnames(AccountContactRelation) <- c("Primary_Company__pc", "ContactId")

AccountContactRelation$exists <- as.numeric(do.call(paste0, AccountContactRelation) %in% do.call(paste0,PrimaryCompany))
AccountContactRelation <- AccountContactRelation %>% filter(exists == "1")

AllAccounts <- left_join(AllAccounts, AccountContactRelation, by=c("ContactId"="ContactId"))
AllAccounts$exists[is.na(AllAccounts$exists)] <- 0
AllAccounts$JanuaryQuestion <- ifelse(AllAccounts$RecordType == "Business Account", 1, AllAccounts$exists)


##PULL FEBRUARY TASKS AND QUESTIONS##
WhatIdFebruaryTasks <- sf_query("SELECT WhatId FROM Task where Date_Completed__c > 2025-01-31 and Date_Completed__c < 2025-03-01 and WhatId not in ('')")
WhoIdFebruaryTasks <- sf_query("SELECT WhoId FROM Task where Date_Completed__c > 2025-01-31 and Date_Completed__c < 2025-03-01 and WhoId not in ('')")
WhatIdFebruaryTasks <- distinct(WhatIdFebruaryTasks)
WhoIdFebruaryTasks <- distinct(WhoIdFebruaryTasks)
WhoIdFebruaryTasks$FebContactedAccount <- 1
WhatIdFebruaryTasks$FebContactedOpp <- 1
WhoIdFebruaryTasks <- filter(WhoIdFebruaryTasks, (WhoId %in% AllAccounts$ContactId))
WhatIdFebruaryTasks <- filter(WhatIdFebruaryTasks, (WhatId %in% AllAccounts$Id))
AllAccounts <- left_join(AllAccounts, WhoIdFebruaryTasks, by=c("ContactId"="WhoId"))
AllAccounts <- left_join(AllAccounts, WhatIdFebruaryTasks, by=c("Id"="WhatId"))
AllAccounts$FebContactedAccount[is.na(AllAccounts$FebContactedAccount)] <- 0
AllAccounts$FebContactedOpp[is.na(AllAccounts$FebContactedOpp)] <- 0
AllAccounts$FebruaryContacted <- AllAccounts$FebContactedAccount + AllAccounts$FebContactedOpp
AllAccounts$FebruaryContacted <- ifelse(AllAccounts$FebruaryContacted>0,1,0)

##February TASKS ON ACCOUNTS RELATED TO BUSINESS ACCOUNTS##
expr_Id300 <- paste0("('", paste0(BusinessAccounts$Id[1:nrow(BusinessAccounts)], collapse = "','"),"')")
BusinessContacts <- sf_query(paste0("Select OpportunityId, ContactId from OpportunityContactRole where OpportunityId in ",expr_Id300))
expr_Id3000 <- paste0("('", paste0(BusinessContacts$ContactId[1:nrow(BusinessContacts)], collapse = "','"),"')")
BusinessContactAccounts <- sf_query(paste0("Select Id, Contact_ID__pc from Account where Contact_ID__pc in ",expr_Id3000))
BusinessContactAccounts <- left_join(BusinessContactAccounts, BusinessContacts, by = c("Contact_ID__pc"="ContactId"))
BusinessFebruaryTasks <- sf_query(paste0("Select WhoId from Task where Date_Completed__c > 2024-01-31 and Date_Completed__c < 2025-03-01 and WhoId in",expr_Id3000))
BusinessFebruaryTasks <- distinct(BusinessFebruaryTasks)
BusinessFebruaryTasks <- left_join(BusinessFebruaryTasks, BusinessContactAccounts, by = c("WhoId" = "Contact_ID__pc"))
AllAccounts$FebruaryContacted2 <- ifelse(AllAccounts$ContactId %in% BusinessFebruaryTasks$WhoId,1,0)
AllAccounts$FebruaryContacted <- AllAccounts$FebruaryContacted + AllAccounts$FebruaryContacted2
AllAccounts$FebruaryContacted <- ifelse(AllAccounts$FebruaryContacted>0,1,0)
AllAccounts <- subset(AllAccounts, select = -FebruaryContacted2)


MaritalStatus0 <- sf_query(paste0("Select Id, Marital_Status__c from Account where Marital_Status__c not in ('') and Id in ",expr_Id200))
MaritalStatus1 <- sf_query(paste0("Select Id, Marital_Status__c from Account where Marital_Status__c not in ('') and Id in ",expr_Id201))
MaritalStatus2 <- sf_query(paste0("Select Id, Marital_Status__c from Account where Marital_Status__c not in ('') and Id in ",expr_Id202))
MaritalStatus3 <- sf_query(paste0("Select Id, Marital_Status__c from Account where Marital_Status__c not in ('') and Id in ",expr_Id203))
MaritalStatus4 <- sf_query(paste0("Select Id, Marital_Status__c from Account where Marital_Status__c not in ('') and Id in ",expr_Id204))
MaritalStatus5 <- sf_query(paste0("Select Id, Marital_Status__c from Account where Marital_Status__c not in ('') and Id in ",expr_Id205))
MaritalStatus6 <- sf_query(paste0("Select Id, Marital_Status__c from Account where Marital_Status__c not in ('') and Id in ",expr_Id206))
MaritalStatus7 <- sf_query(paste0("Select Id, Marital_Status__c from Account where Marital_Status__c not in ('') and Id in ",expr_Id207))
MaritalStatus8 <- sf_query(paste0("Select Id, Marital_Status__c from Account where Marital_Status__c not in ('') and Id in ",expr_Id208))
MaritalStatus9 <- sf_query(paste0("Select Id, Marital_Status__c from Account where Marital_Status__c not in ('') and Id in ",expr_Id209))
MaritalStatus10 <- sf_query(paste0("Select Id, Marital_Status__c from Account where Marital_Status__c not in ('') and Id in ",expr_Id210))
MaritalStatus11 <- sf_query(paste0("Select Id, Marital_Status__c from Account where Marital_Status__c not in ('') and Id in ",expr_Id211))
MaritalStatus12 <- sf_query(paste0("Select Id, Marital_Status__c from Account where Marital_Status__c not in ('') and Id in ",expr_Id212))
MaritalStatus13 <- sf_query(paste0("Select Id, Marital_Status__c from Account where Marital_Status__c not in ('') and Id in ",expr_Id213))
MaritalStatus <- rbind(MaritalStatus0, MaritalStatus1, MaritalStatus2, MaritalStatus3, MaritalStatus4, MaritalStatus5, MaritalStatus6, MaritalStatus7, MaritalStatus8, MaritalStatus9, MaritalStatus10, MaritalStatus11, MaritalStatus12, MaritalStatus13)
MaritalStatus <- left_join(MaritalStatus, AllAccounts, by=c("Id"="AccountId"))
MaritalStatus <- MaritalStatus[c("Id.y", "Marital_Status__c")]
MaritalStatus <- left_join(AllAccounts, MaritalStatus, by=c("Id" = "Id.y"))
MaritalStatus <- filter(MaritalStatus, Marital_Status__c != '')
MaritalStatus$Marital_Status__c <-1
MaritalStatus <- MaritalStatus[c("Id", "Marital_Status__c")]
colnames(MaritalStatus) <- c("OpportunityId", "Marital_Status__c")

expr_Id300 <- paste0("('", paste0(BusinessAccounts$Id[1:nrow(BusinessAccounts)], collapse = "','"),"')")
BusinessContacts <- sf_query(paste0("Select OpportunityId, ContactId from OpportunityContactRole where OpportunityId in ",expr_Id300))
expr_Id301 <- paste0("('", paste0(BusinessContacts$ContactId[1:nrow(BusinessContacts)], collapse = "','"),"')")
BusinessContactMarital <- sf_query(paste0("Select Contact_ID__pc, Id, Marital_Status__c from Account where Marital_Status__c not in ('') and Contact_ID__pc in ",expr_Id301))
BusinessContactMarital <- left_join(BusinessContacts, BusinessContactMarital, by=c("ContactId"="Contact_ID__pc"))
BusinessContactMarital <- BusinessContactMarital[c("OpportunityId", "Marital_Status__c")]
BusinessContactMarital <- filter(BusinessContactMarital, Marital_Status__c != '')
BusinessContactMarital$Marital_Status__c <- 1

MaritalStatus <- rbind(MaritalStatus, BusinessContactMarital)
AllAccounts <- left_join(AllAccounts, MaritalStatus, by=c("Id"="OpportunityId"))
AllAccounts$Marital_Status__c[is.na(AllAccounts$Marital_Status__c)] <- 0

##PULL MARCH TASKS AND QUESTIONS##
WhatIdMarchTasks <- sf_query("SELECT WhatId FROM Task where Date_Completed__c > 2025-02-28 and Date_Completed__c < 2025-04-01 and WhatId not in ('')")
WhoIdMarchTasks <- sf_query("SELECT WhoId FROM Task where Date_Completed__c > 2025-02-28 and Date_Completed__c < 2025-04-01 and WhoId not in ('')")
WhatIdMarchTasks <- distinct(WhatIdMarchTasks)
WhoIdMarchTasks <- distinct(WhoIdMarchTasks)
WhoIdMarchTasks$MarContactedAccount <- 1
WhatIdMarchTasks$MarContactedOpp <- 1
WhoIdMarchTasks <- filter(WhoIdMarchTasks, (WhoId %in% AllAccounts$ContactId))
WhatIdMarchTasks <- filter(WhatIdMarchTasks, (WhatId %in% AllAccounts$Id))
AllAccounts <- left_join(AllAccounts, WhoIdMarchTasks, by=c("ContactId"="WhoId"))
AllAccounts <- left_join(AllAccounts, WhatIdMarchTasks, by=c("Id"="WhatId"))
AllAccounts$MarContactedAccount[is.na(AllAccounts$MarContactedAccount)] <- 0
AllAccounts$MarContactedOpp[is.na(AllAccounts$MarContactedOpp)] <- 0
AllAccounts$MarchContacted <- AllAccounts$MarContactedAccount + AllAccounts$MarContactedOpp
AllAccounts$MarchContacted <- ifelse(AllAccounts$MarchContacted>0,1,0)

##March TASKS ON ACCOUNTS RELATED TO BUSINESS ACCOUNTS##
expr_Id300 <- paste0("('", paste0(BusinessAccounts$Id[1:nrow(BusinessAccounts)], collapse = "','"),"')")
BusinessContacts <- sf_query(paste0("Select OpportunityId, ContactId from OpportunityContactRole where OpportunityId in ",expr_Id300))
expr_Id3000 <- paste0("('", paste0(BusinessContacts$ContactId[1:nrow(BusinessContacts)], collapse = "','"),"')")
BusinessContactAccounts <- sf_query(paste0("Select Id, Contact_ID__pc from Account where Contact_ID__pc in ",expr_Id3000))
BusinessContactAccounts <- left_join(BusinessContactAccounts, BusinessContacts, by = c("Contact_ID__pc"="ContactId"))
BusinessMarchTasks <- sf_query(paste0("Select WhoId from Task where Date_Completed__c > 2024-02-28 and Date_Completed__c < 2025-04-01 and WhoId in",expr_Id3000))
BusinessMarchTasks <- distinct(BusinessMarchTasks)
BusinessMarchTasks <- left_join(BusinessMarchTasks, BusinessContactAccounts, by = c("WhoId" = "Contact_ID__pc"))
AllAccounts$MarchContacted2 <- ifelse(AllAccounts$ContactId %in% BusinessMarchTasks$WhoId,1,0)
AllAccounts$MarchContacted <- AllAccounts$MarchContacted + AllAccounts$MarchContacted2
AllAccounts$MarchContacted <- ifelse(AllAccounts$MarchContacted>0,1,0)
AllAccounts <- subset(AllAccounts, select = -MarchContacted2)

AlmaMater0 <- sf_query(paste0("Select Id, Alma_Mater__c from Account where Alma_Mater__c not in ('') and Id in ",expr_Id200))
AlmaMater1 <- sf_query(paste0("Select Id, Alma_Mater__c from Account where Alma_Mater__c not in ('') and Id in ",expr_Id201))
AlmaMater2 <- sf_query(paste0("Select Id, Alma_Mater__c from Account where Alma_Mater__c not in ('') and Id in ",expr_Id202))
AlmaMater3 <- sf_query(paste0("Select Id, Alma_Mater__c from Account where Alma_Mater__c not in ('') and Id in ",expr_Id203))
AlmaMater4 <- sf_query(paste0("Select Id, Alma_Mater__c from Account where Alma_Mater__c not in ('') and Id in ",expr_Id204))
AlmaMater5 <- sf_query(paste0("Select Id, Alma_Mater__c from Account where Alma_Mater__c not in ('') and Id in ",expr_Id205))
AlmaMater6 <- sf_query(paste0("Select Id, Alma_Mater__c from Account where Alma_Mater__c not in ('') and Id in ",expr_Id206))
AlmaMater7 <- sf_query(paste0("Select Id, Alma_Mater__c from Account where Alma_Mater__c not in ('') and Id in ",expr_Id207))
AlmaMater8 <- sf_query(paste0("Select Id, Alma_Mater__c from Account where Alma_Mater__c not in ('') and Id in ",expr_Id208))
AlmaMater9 <- sf_query(paste0("Select Id, Alma_Mater__c from Account where Alma_Mater__c not in ('') and Id in ",expr_Id209))
AlmaMater10 <- sf_query(paste0("Select Id, Alma_Mater__c from Account where Alma_Mater__c not in ('') and Id in ",expr_Id210))
AlmaMater11 <- sf_query(paste0("Select Id, Alma_Mater__c from Account where Alma_Mater__c not in ('') and Id in ",expr_Id211))
AlmaMater12 <- sf_query(paste0("Select Id, Alma_Mater__c from Account where Alma_Mater__c not in ('') and Id in ",expr_Id212))
AlmaMater13 <- sf_query(paste0("Select Id, Alma_Mater__c from Account where Alma_Mater__c not in ('') and Id in ",expr_Id213))
AlmaMater <- rbind(AlmaMater0, AlmaMater1, AlmaMater2, AlmaMater3, AlmaMater4, AlmaMater5, AlmaMater6, AlmaMater7, AlmaMater8, AlmaMater9, AlmaMater10, AlmaMater11, AlmaMater12, AlmaMater13)
AlmaMater <- left_join(AlmaMater, AllAccounts, by=c("Id"="AccountId"))
AlmaMater <- AlmaMater[c("Id.y", "Alma_Mater__c")]
AlmaMater <- left_join(AllAccounts, AlmaMater, by=c("Id" = "Id.y"))
AlmaMater <- filter(AlmaMater, Alma_Mater__c != '')
AlmaMater$Alma_Mater__c <-1
AlmaMater <- AlmaMater[c("Id", "Alma_Mater__c")]
colnames(AlmaMater) <- c("OpportunityId", "Alma_Mater__c")

expr_Id300 <- paste0("('", paste0(BusinessAccounts$Id[1:nrow(BusinessAccounts)], collapse = "','"),"')")
BusinessContacts <- sf_query(paste0("Select OpportunityId, ContactId from OpportunityContactRole where OpportunityId in ",expr_Id300))
expr_Id301 <- paste0("('", paste0(BusinessContacts$ContactId[1:nrow(BusinessContacts)], collapse = "','"),"')")
BusinessContactAlmaMater <- sf_query(paste0("Select Contact_ID__pc, Id, Alma_Mater__c from Account where Alma_Mater__c not in ('') and Contact_ID__pc in ",expr_Id301))
BusinessContactAlmaMater <- left_join(BusinessContacts, BusinessContactAlmaMater, by=c("ContactId"="Contact_ID__pc"))
BusinessContactAlmaMater <- BusinessContactAlmaMater[c("OpportunityId", "Alma_Mater__c")]
BusinessContactAlmaMater <- filter(BusinessContactAlmaMater, Alma_Mater__c != '')
BusinessContactAlmaMater$Alma_Mater__c <- 1

AlmaMater <- rbind(AlmaMater, BusinessContactAlmaMater)
AllAccounts <- left_join(AllAccounts, AlmaMater, by=c("Id"="OpportunityId"))
AllAccounts$Alma_Mater__c[is.na(AllAccounts$Alma_Mater__c)] <- 0

######APRIL#######
##PULL APRIL TASKS AND QUESTIONS##
WhatIdAprilTasks <- sf_query("SELECT WhatId FROM Task where Date_Completed__c > 2025-03-31 and Date_Completed__c < 2025-05-01 and WhatId not in ('')")
WhoIdAprilTasks <- sf_query("SELECT WhoId FROM Task where Date_Completed__c > 2025-03-31 and Date_Completed__c < 2025-05-01 and WhoId not in ('')")
WhatIdAprilTasks <- distinct(WhatIdAprilTasks)
WhoIdAprilTasks <- distinct(WhoIdAprilTasks)
WhoIdAprilTasks$AprContactedAccount <- 1
WhatIdAprilTasks$AprContactedOpp <- 1
WhoIdAprilTasks <- filter(WhoIdAprilTasks, (WhoId %in% AllAccounts$ContactId))
WhatIdAprilTasks <- filter(WhatIdAprilTasks, (WhatId %in% AllAccounts$Id))
AllAccounts <- left_join(AllAccounts, WhoIdAprilTasks, by=c("ContactId"="WhoId"))
AllAccounts <- left_join(AllAccounts, WhatIdAprilTasks, by=c("Id"="WhatId"))
AllAccounts$AprContactedAccount[is.na(AllAccounts$AprContactedAccount)] <- 0
AllAccounts$AprContactedOpp[is.na(AllAccounts$AprContactedOpp)] <- 0
AllAccounts$AprilContacted <- AllAccounts$AprContactedAccount + AllAccounts$AprContactedOpp
AllAccounts$AprilContacted <- ifelse(AllAccounts$AprilContacted>0,1,0)

##April TASKS ON ACCOUNTS RELATED TO BUSINESS ACCOUNTS##
expr_Id300 <- paste0("('", paste0(BusinessAccounts$Id[1:nrow(BusinessAccounts)], collapse = "','"),"')")
BusinessContacts <- sf_query(paste0("Select OpportunityId, ContactId from OpportunityContactRole where OpportunityId in ",expr_Id300))
expr_Id3000 <- paste0("('", paste0(BusinessContacts$ContactId[1:nrow(BusinessContacts)], collapse = "','"),"')")
BusinessContactAccounts <- sf_query(paste0("Select Id, Contact_ID__pc from Account where Contact_ID__pc in ",expr_Id3000))
BusinessContactAccounts <- left_join(BusinessContactAccounts, BusinessContacts, by = c("Contact_ID__pc"="ContactId"))
BusinessAprilTasks <- sf_query(paste0("Select WhoId from Task where Date_Completed__c > 2024-03-31 and Date_Completed__c < 2025-05-01 and WhoId in",expr_Id3000))
BusinessAprilTasks <- distinct(BusinessAprilTasks)
BusinessAprilTasks <- left_join(BusinessAprilTasks, BusinessContactAccounts, by = c("WhoId" = "Contact_ID__pc"))
AllAccounts$AprilContacted2 <- ifelse(AllAccounts$ContactId %in% BusinessAprilTasks$WhoId,1,0)
AllAccounts$AprilContacted <- AllAccounts$AprilContacted + AllAccounts$AprilContacted2
AllAccounts$AprilContacted <- ifelse(AllAccounts$AprilContacted>0,1,0)
AllAccounts <- subset(AllAccounts, select = -AprilContacted2)

FavSTM0 <- sf_query(paste0("Select Id, Favorite_Part_of_Being_a_Member__c from Account where Favorite_Part_of_Being_a_Member__c not in ('') and Id in ",expr_Id200))
FavSTM1 <- sf_query(paste0("Select Id, Favorite_Part_of_Being_a_Member__c from Account where Favorite_Part_of_Being_a_Member__c not in ('') and Id in ",expr_Id201))
FavSTM2 <- sf_query(paste0("Select Id, Favorite_Part_of_Being_a_Member__c from Account where Favorite_Part_of_Being_a_Member__c not in ('') and Id in ",expr_Id202))
FavSTM3 <- sf_query(paste0("Select Id, Favorite_Part_of_Being_a_Member__c from Account where Favorite_Part_of_Being_a_Member__c not in ('') and Id in ",expr_Id203))
FavSTM4 <- sf_query(paste0("Select Id, Favorite_Part_of_Being_a_Member__c from Account where Favorite_Part_of_Being_a_Member__c not in ('') and Id in ",expr_Id204))
FavSTM5 <- sf_query(paste0("Select Id, Favorite_Part_of_Being_a_Member__c from Account where Favorite_Part_of_Being_a_Member__c not in ('') and Id in ",expr_Id205))
FavSTM6 <- sf_query(paste0("Select Id, Favorite_Part_of_Being_a_Member__c from Account where Favorite_Part_of_Being_a_Member__c not in ('') and Id in ",expr_Id206))
FavSTM7 <- sf_query(paste0("Select Id, Favorite_Part_of_Being_a_Member__c from Account where Favorite_Part_of_Being_a_Member__c not in ('') and Id in ",expr_Id207))
FavSTM8 <- sf_query(paste0("Select Id, Favorite_Part_of_Being_a_Member__c from Account where Favorite_Part_of_Being_a_Member__c not in ('') and Id in ",expr_Id208))
FavSTM9 <- sf_query(paste0("Select Id, Favorite_Part_of_Being_a_Member__c from Account where Favorite_Part_of_Being_a_Member__c not in ('') and Id in ",expr_Id209))
FavSTM10 <- sf_query(paste0("Select Id, Favorite_Part_of_Being_a_Member__c from Account where Favorite_Part_of_Being_a_Member__c not in ('') and Id in ",expr_Id210))
FavSTM11 <- sf_query(paste0("Select Id, Favorite_Part_of_Being_a_Member__c from Account where Favorite_Part_of_Being_a_Member__c not in ('') and Id in ",expr_Id211))
FavSTM12 <- sf_query(paste0("Select Id, Favorite_Part_of_Being_a_Member__c from Account where Favorite_Part_of_Being_a_Member__c not in ('') and Id in ",expr_Id212))
FavSTM13 <- sf_query(paste0("Select Id, Favorite_Part_of_Being_a_Member__c from Account where Favorite_Part_of_Being_a_Member__c not in ('') and Id in ",expr_Id213))
FavSTM <- rbind(FavSTM0, FavSTM1, FavSTM2, FavSTM3, FavSTM4, FavSTM5, FavSTM6, FavSTM7, FavSTM8, FavSTM9, FavSTM10, FavSTM11, FavSTM12, FavSTM13)
FavSTM <- left_join(FavSTM, AllAccounts, by=c("Id"="AccountId"))
FavSTM <- FavSTM[c("Id.y", "Favorite_Part_of_Being_a_Member__c")]
FavSTM <- left_join(AllAccounts, FavSTM, by=c("Id" = "Id.y"))
FavSTM <- filter(FavSTM, Favorite_Part_of_Being_a_Member__c != '')
FavSTM$Favorite_Part_of_Being_a_Member__c <-1
FavSTM <- FavSTM[c("Id", "Favorite_Part_of_Being_a_Member__c")]
colnames(FavSTM) <- c("OpportunityId", "Favorite_Part_of_Being_a_Member__c")

expr_Id300 <- paste0("('", paste0(BusinessAccounts$Id[1:nrow(BusinessAccounts)], collapse = "','"),"')")
BusinessContacts <- sf_query(paste0("Select OpportunityId, ContactId from OpportunityContactRole where OpportunityId in ",expr_Id300))
expr_Id301 <- paste0("('", paste0(BusinessContacts$ContactId[1:nrow(BusinessContacts)], collapse = "','"),"')")
BusinessContactFavSTM <- sf_query(paste0("Select Contact_ID__pc, Id, Favorite_Part_of_Being_a_Member__c from Account where Favorite_Part_of_Being_a_Member__c not in ('') and Contact_ID__pc in ",expr_Id301))
BusinessContactFavSTM <- left_join(BusinessContacts, BusinessContactFavSTM, by=c("ContactId"="Contact_ID__pc"))
BusinessContactFavSTM <- BusinessContactFavSTM[c("OpportunityId", "Favorite_Part_of_Being_a_Member__c")]
BusinessContactFavSTM <- filter(BusinessContactFavSTM, Favorite_Part_of_Being_a_Member__c != '')
BusinessContactFavSTM$Favorite_Part_of_Being_a_Member__c <- 1

FavSTM <- rbind(FavSTM, BusinessContactFavSTM)
AllAccounts <- left_join(AllAccounts, FavSTM, by=c("Id"="OpportunityId"))
AllAccounts$Favorite_Part_of_Being_a_Member__c[is.na(AllAccounts$Favorite_Part_of_Being_a_Member__c)] <- 0

######MAY#######
##PULL MAY TASKS AND QUESTIONS##
WhatIdMayTasks <- sf_query("SELECT WhatId FROM Task where Date_Completed__c > 2025-04-30 and Date_Completed__c < 2025-06-01 and WhatId not in ('')")
WhoIdMayTasks <- sf_query("SELECT WhoId FROM Task where Date_Completed__c > 2025-04-30 and Date_Completed__c < 2025-06-01 and WhoId not in ('')")
WhatIdMayTasks <- distinct(WhatIdMayTasks)
WhoIdMayTasks <- distinct(WhoIdMayTasks)
WhoIdMayTasks$MayContactedAccount <- 1
WhatIdMayTasks$MayContactedOpp <- 1
WhoIdMayTasks <- filter(WhoIdMayTasks, (WhoId %in% AllAccounts$ContactId))
WhatIdMayTasks <- filter(WhatIdMayTasks, (WhatId %in% AllAccounts$Id))
AllAccounts <- left_join(AllAccounts, WhoIdMayTasks, by=c("ContactId"="WhoId"))
AllAccounts <- left_join(AllAccounts, WhatIdMayTasks, by=c("Id"="WhatId"))
AllAccounts$MayContactedAccount[is.na(AllAccounts$MayContactedAccount)] <- 0
AllAccounts$MayContactedOpp[is.na(AllAccounts$MayContactedOpp)] <- 0
AllAccounts$MayContacted <- AllAccounts$MayContactedAccount + AllAccounts$MayContactedOpp
AllAccounts$MayContacted <- ifelse(AllAccounts$MayContacted>0,1,0)

##May TASKS ON ACCOUNTS RELATED TO BUSINESS ACCOUNTS##
expr_Id300 <- paste0("('", paste0(BusinessAccounts$Id[1:nrow(BusinessAccounts)], collapse = "','"),"')")
BusinessContacts <- sf_query(paste0("Select OpportunityId, ContactId from OpportunityContactRole where OpportunityId in ",expr_Id300))
expr_Id3000 <- paste0("('", paste0(BusinessContacts$ContactId[1:nrow(BusinessContacts)], collapse = "','"),"')")
BusinessContactAccounts <- sf_query(paste0("Select Id, Contact_ID__pc from Account where Contact_ID__pc in ",expr_Id3000))
BusinessContactAccounts <- left_join(BusinessContactAccounts, BusinessContacts, by = c("Contact_ID__pc"="ContactId"))
BusinessMayTasks <- sf_query(paste0("Select WhoId from Task where Date_Completed__c > 2024-04-30 and Date_Completed__c < 2025-06-01 and WhoId in",expr_Id3000))
BusinessMayTasks <- distinct(BusinessMayTasks)
BusinessMayTasks <- left_join(BusinessMayTasks, BusinessContactAccounts, by = c("WhoId" = "Contact_ID__pc"))
AllAccounts$MayContacted2 <- ifelse(AllAccounts$ContactId %in% BusinessMayTasks$WhoId,1,0)
AllAccounts$MayContacted <- AllAccounts$MayContacted + AllAccounts$MayContacted2
AllAccounts$MayContacted <- ifelse(AllAccounts$MayContacted>0,1,0)
AllAccounts <- subset(AllAccounts, select = -MayContacted2)

DSL0 <- sf_query(paste0("Select Id, Desired_Seat_Location__c from Account where Desired_Seat_Location__c not in ('') and Id in ",expr_Id200))
DSL1 <- sf_query(paste0("Select Id, Desired_Seat_Location__c from Account where Desired_Seat_Location__c not in ('') and Id in ",expr_Id201))
DSL2 <- sf_query(paste0("Select Id, Desired_Seat_Location__c from Account where Desired_Seat_Location__c not in ('') and Id in ",expr_Id202))
DSL3 <- sf_query(paste0("Select Id, Desired_Seat_Location__c from Account where Desired_Seat_Location__c not in ('') and Id in ",expr_Id203))
DSL4 <- sf_query(paste0("Select Id, Desired_Seat_Location__c from Account where Desired_Seat_Location__c not in ('') and Id in ",expr_Id204))
DSL5 <- sf_query(paste0("Select Id, Desired_Seat_Location__c from Account where Desired_Seat_Location__c not in ('') and Id in ",expr_Id205))
DSL6 <- sf_query(paste0("Select Id, Desired_Seat_Location__c from Account where Desired_Seat_Location__c not in ('') and Id in ",expr_Id206))
DSL7 <- sf_query(paste0("Select Id, Desired_Seat_Location__c from Account where Desired_Seat_Location__c not in ('') and Id in ",expr_Id207))
DSL8 <- sf_query(paste0("Select Id, Desired_Seat_Location__c from Account where Desired_Seat_Location__c not in ('') and Id in ",expr_Id208))
DSL9 <- sf_query(paste0("Select Id, Desired_Seat_Location__c from Account where Desired_Seat_Location__c not in ('') and Id in ",expr_Id209))
DSL10 <- sf_query(paste0("Select Id, Desired_Seat_Location__c from Account where Desired_Seat_Location__c not in ('') and Id in ",expr_Id210))
DSL11 <- sf_query(paste0("Select Id, Desired_Seat_Location__c from Account where Desired_Seat_Location__c not in ('') and Id in ",expr_Id211))
DSL12 <- sf_query(paste0("Select Id, Desired_Seat_Location__c from Account where Desired_Seat_Location__c not in ('') and Id in ",expr_Id212))
DSL13 <- sf_query(paste0("Select Id, Desired_Seat_Location__c from Account where Desired_Seat_Location__c not in ('') and Id in ",expr_Id213))
DSL <- rbind(DSL0, DSL1, DSL2, DSL3, DSL4, DSL5, DSL6, DSL7, DSL8, DSL9, DSL10, DSL11, DSL12, DSL13)
DSL <- left_join(DSL, AllAccounts, by=c("Id"="AccountId"))
DSL <- distinct(DSL)
DSL <- DSL[c("Id.y", "Desired_Seat_Location__c")]
DSL <- left_join(AllAccounts, DSL, by=c("Id" = "Id.y"))
DSL <- filter(DSL, Desired_Seat_Location__c != '')
DSL$Desired_Seat_Location__c <-1
DSL <- DSL[c("Id", "Desired_Seat_Location__c")]
colnames(DSL) <- c("OpportunityId", "Desired_Seat_Location__c")

expr_Id300 <- paste0("('", paste0(BusinessAccounts$Id[1:nrow(BusinessAccounts)], collapse = "','"),"')")
BusinessContacts <- sf_query(paste0("Select OpportunityId, ContactId from OpportunityContactRole where OpportunityId in ",expr_Id300))
expr_Id301 <- paste0("('", paste0(BusinessContacts$ContactId[1:nrow(BusinessContacts)], collapse = "','"),"')")
BusinessContactDSL <- sf_query(paste0("Select Contact_ID__pc, Id, Desired_Seat_Location__c from Account where Desired_Seat_Location__c not in ('') and Contact_ID__pc in ",expr_Id301))
BusinessContactDSL <- left_join(BusinessContacts, BusinessContactDSL, by=c("ContactId"="Contact_ID__pc"))
BusinessContactDSL <- BusinessContactDSL[c("OpportunityId", "Desired_Seat_Location__c")]
BusinessContactDSL <- filter(BusinessContactDSL, Desired_Seat_Location__c != '')
BusinessContactDSL$Desired_Seat_Location__c <- 1

DSL <- rbind(DSL, BusinessContactDSL)
DSL <- distinct(DSL)
AllAccounts <- left_join(AllAccounts, DSL, by=c("Id"="OpportunityId"))
AllAccounts$Desired_Seat_Location__c[is.na(AllAccounts$Desired_Seat_Location__c)] <- 0



##CREATE FINAL DATAFRAME##
Final <- AllAccounts[c("Id", "AccountName", "AccountId", "Product", "TicketingId", "OwnerName", "JanuaryContacted", "JanuaryQuestion", "FebruaryContacted", "Marital_Status__c", "MarchContacted", "Alma_Mater__c", "AprilContacted", "Favorite_Part_of_Being_a_Member__c", "MayContacted", "Desired_Seat_Location__c")]
Final <- distinct(Final)

JanuaryContacted2 <- Final %>% group_by(Id) %>% summarise(JanuaryContacted = max(JanuaryContacted), na.rm = TRUE) 
JanuaryContacted2 <- distinct(JanuaryContacted2)
JanuaryContacted2 <- JanuaryContacted2[c("Id", "JanuaryContacted")]
JanuaryQuestion2 <- Final %>% group_by(Id) %>% summarise(JanuaryQuestion = max(JanuaryQuestion), na.rm = TRUE) 
JanuaryQuestion2 <- distinct(JanuaryQuestion2)
JanuaryQuestion2 <- JanuaryQuestion2[c("Id", "JanuaryQuestion")]
FebruaryContacted2 <- Final %>% group_by(Id) %>% summarise(FebruaryContacted = max(FebruaryContacted), na.rm = TRUE)
FebruaryContacted2 <- distinct(FebruaryContacted2)
FebruaryContacted2 <- FebruaryContacted2[c("Id", "FebruaryContacted")]
Marital_Status <- Final %>% group_by(Id) %>% summarise(Marital_Status__c = max(Marital_Status__c), na.rm = TRUE)
Marital_Status <- distinct(Marital_Status)
Marital_Status <- Marital_Status[c("Id", "Marital_Status__c")]
MarchContacted2 <- Final %>% group_by(Id) %>% summarise(MarchContacted = max(MarchContacted), na.rm = TRUE)
MarchContacted2 <- distinct(MarchContacted2)
MarchContacted2 <- MarchContacted2[c("Id", "MarchContacted")]
AlmaMater <- Final %>% group_by(Id) %>% summarise(Alma_Mater__c = max(Alma_Mater__c), na.rm = TRUE)
AlmaMater <- distinct(AlmaMater)
AlmaMater <- AlmaMater[c("Id", "Alma_Mater__c")]
AprilContacted2 <- Final %>% group_by(Id) %>% summarise(AprilContacted = max(AprilContacted), na.rm = TRUE)
AprilContacted2 <- distinct(AprilContacted2)
AprilContacted2 <- AprilContacted2[c("Id", "AprilContacted")]
FavSTM <- Final %>% group_by(Id) %>% summarise(Favorite_Part_of_Being_a_Member__c = max(Favorite_Part_of_Being_a_Member__c), na.rm = TRUE)
FavSTM <- distinct(FavSTM)
FavSTM <- FavSTM[c("Id", "Favorite_Part_of_Being_a_Member__c")]
MayContacted2 <- Final %>% group_by(Id) %>% summarise(MayContacted = max(MayContacted), na.rm = TRUE)
MayContacted2 <- distinct(MayContacted2)
MayContacted2 <- MayContacted2[c("Id", "MayContacted")]
DSL <- Final %>% group_by(Id) %>% summarise(Desired_Seat_Location__c = max(Desired_Seat_Location__c), na.rm = TRUE)
DSL <- distinct(DSL)
DSL <- DSL[c("Id", "Desired_Seat_Location__c")]




Final <- AllAccounts[c("Id", "AccountName", "AccountId", "Product", "TicketingId", "OwnerName")]
Final <- distinct(Final)
Final <- left_join(Final, JanuaryContacted2, by=c("Id" = "Id"))
Final <- left_join(Final, JanuaryQuestion2, by=c("Id" = "Id"))
Final <- left_join(Final, FebruaryContacted2, by=c("Id" = "Id"))
Final <- left_join(Final, Marital_Status, by=c("Id" = "Id"))
Final <- left_join(Final, MarchContacted2, by=c("Id" = "Id"))
Final <- left_join(Final, AlmaMater, by=c("Id" = "Id"))
Final <- left_join(Final, AprilContacted2, by=c("Id" = "Id"))
Final <- left_join(Final, FavSTM, by=c("Id" = "Id"))
Final <- left_join(Final, MayContacted2, by=c("Id" = "Id"))
Final <- left_join(Final, DSL, by=c("Id" = "Id"))



fwrite(Final, file = '/Users/marino/Desktop/MaintenanceTracker.csv')
