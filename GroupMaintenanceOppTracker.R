
query <- sf_query("Select Id, OwnerId, Account_Name_Slack__c, AccountId, Event__c from Opportunity where Start_Season_Name__c in ('2025') and Product__c in ('Group') and StageName in ('Closed Won')")
query2 <- sf_query("SELECT Id, Name, agilitek__Date__c FROM agilitek__Event__c")
query3 <- left_join(query, query2, by = c("Event__c" = "Id"))
query3 <- query3 %>% filter(agilitek__Date__c >= 2025-01-01)

Owners <- sf_query("Select Name, Id from User where IsActive = TRUE")
query3 <- left_join(query3, Owners, by=c("OwnerId"="Id"))

AccountIds <- data.frame(query3$AccountId)
AccountIds <- na.omit(AccountIds)
expr_Id<- paste0("('", paste0(AccountIds$query3.AccountId[1:nrow(AccountIds)], collapse = "','"),"')")
BusinessPersonal <- sf_query(paste0("Select Name, Id, Contact_ID__pc, Primary_Ticketing_ID__pc, RecordTypeId from Account where Id in ",expr_Id))
RecordType <- sf_query("Select Id, Name from RecordType")
BusinessPersonal <- left_join(BusinessPersonal, RecordType, by=c("RecordTypeId"="Id"))
colnames(BusinessPersonal) <- c("AccountId", "ContactId", "AccountName", "TicketingId", "RecordTypeId", "RecordType")
query <- left_join(query3,BusinessPersonal, by=c("AccountId"="AccountId"))

colnames(query) <- c("Id", "AccountName", "AccountId", "EventId", "OwnerId", "EventDate", "Event", "OwnerName", "ContactId", "Account Name", "TicketingId", "RecordTypeId", "RecordType")
query <- query[c("Id", "AccountName", "AccountId", "Event","ContactId", "TicketingId", "OwnerId", "OwnerName", "RecordType")]

BusinessAccounts <- filter(query, RecordType =='Business Account')
PersonAccounts <- filter (query, RecordType =='Person Account')

expr_Id100<- paste0("('", paste0(BusinessAccounts$Id[1:nrow(BusinessAccounts)], collapse = "','"),"')")
ContactRoleQuery <- sf_query(paste0("SELECT ContactId, OpportunityId FROM OpportunityContactRole where OpportunityId in ",expr_Id100))
BusinessAccounts <- left_join(BusinessAccounts, ContactRoleQuery, by=c("Id"="OpportunityId"))
BusinessAccounts <- BusinessAccounts[c("Id", "AccountName", "AccountId", "Event", "ContactId.y","TicketingId", "OwnerId", "OwnerName", "RecordType")]
colnames(BusinessAccounts) <- c("Id", "AccountName", "AccountId", "Event", "ContactId", "TicketingId", "OwnerId", "OwnerName", "RecordType")

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
expr_Id200<- paste0("('", paste0(PersonAccounts$AccountId[1:nrow(PersonAccounts)], collapse = "','"),"')")
PrimaryCompany <- sf_query(paste0("Select Id, Primary_Company__pc from Account where Primary_Company__pc not in ('') and Id in ",expr_Id200))
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


MaritalStatus <- sf_query(paste0("Select Id, Marital_Status__c from Account where Marital_Status__c not in ('') and Id in ",expr_Id200))
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


AlmaMater <- sf_query(paste0("Select Id, Alma_Mater__c from Account where Alma_Mater__c not in ('') and Id in ",expr_Id200))
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

FavSTM <- sf_query(paste0("Select Id, Favorite_Part_of_Being_a_Member__c from Account where Favorite_Part_of_Being_a_Member__c not in ('') and Id in ",expr_Id200))
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

DSL <- sf_query(paste0("Select Id, Desired_Seat_Location__c from Account where Desired_Seat_Location__c not in ('') and Id in ",expr_Id200))
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
Final <- AllAccounts[c("Id", "AccountName", "AccountId", "Event", "TicketingId", "OwnerName", "JanuaryContacted", "JanuaryQuestion", "FebruaryContacted", "Marital_Status__c", "MarchContacted", "Alma_Mater__c", "AprilContacted", "Favorite_Part_of_Being_a_Member__c", "MayContacted", "Desired_Seat_Location__c")]
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




Final <- AllAccounts[c("Id", "AccountName", "AccountId", "Event", "TicketingId", "OwnerName")]
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



fwrite(Final, file = '/Users/marino/Desktop/GroupMaintenanceTracker.csv')
