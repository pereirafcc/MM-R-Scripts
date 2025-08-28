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

##OPT OUTS##
OptOuts <- fread(file = '/Users/marino/Downloads/Opt Outs.csv')
colnames(OptOuts)[3] <- "Email"
OptOuts[OptOuts == ""] <- NA
subset_cols <- OptOuts[, 8:14]
subset_names <- names(OptOuts)[8:14]
OptOuts$LossReason <- subset_names[max.col(!is.na(subset_cols), ties.method = "first")]


OptOuts <- OptOuts %>%
  mutate(
    LossReason = if_else(LossReason == "Ticket Utilization/Does Not Fit Schedule - I found I had too many conflicts with games and couldn't attend", "Schedule / Time", LossReason)
  )

OptOuts <- OptOuts %>%
  mutate(
    LossReason = if_else(LossReason == "Investment Value - I can’t justify the cost of my Membership with FC Cincinnati", "Price / Value", LossReason)
  )

OptOuts <- OptOuts %>%
  mutate(
    LossReason = if_else(LossReason == "Moving out of State/Cincinnati Area", "Moved Out of Town", LossReason)
  )

OptOuts <- OptOuts %>%
  mutate(
    LossReason = if_else(LossReason == "Team Performance/Direction of the Team – The performance of the team is deterring me from renewing", "Team Performance", LossReason)
  )

OptOuts <- OptOuts %>%
  mutate(
    LossReason = if_else(LossReason == "Partial Cancellation – I currently have too many seats and need to drop some from my Membership", "Price / Value", LossReason)
  )

OptOuts <- OptOuts %>%
  mutate(
    LossReason = if_else(LossReason == "Seat Location Not Available / Desirable", "Seat Location Not Available / Desirable", LossReason)
  )

OptOuts <- OptOuts %>%
  mutate(
    LossReason = if_else(LossReason == "Other", "Price / Value", LossReason)
  )

OptOuts$Description <- OptOuts$Other
expr_Id1 <- paste0("('", paste0(OptOuts$Email[1:nrow(OptOuts)], collapse = "','"),"')")
OptOutAccounts <- sf_query(paste0("Select Id, PersonEmail from Account where PersonEmail in ",expr_Id1))
expr_Id2 <- paste0("('", paste0(OptOutAccounts$Id[1:nrow(OptOutAccounts)], collapse = "','"),"')")

OptOutOpps <- sf_query(paste0("SELECT Id, AccountId, Amount, Number_of_Tickets__c, StageName, AddOnRequested__c, UpgradeRequested__c FROM Opportunity where CampaignId in ('701UP00000G2uhdYAB', '701UP00000AOwgDYAT') and AccountId in ",expr_Id2))
OptOutOpps <- OptOutOpps %>% filter(StageName != 'Closed Lost')
OptOutOpps$StageName <- 'Closed Lost'

OptOutOpps <- left_join(OptOutOpps, OptOutAccounts, by=c("AccountId" = "Id"))
OptOutOpps <- left_join(OptOutOpps, OptOuts, by=c("PersonEmail" = "Email"))
OptOutOpps <- OptOutOpps %>% select(1:7, 31:32)

OptOutOpps <- OptOutOpps %>%
  rename(Loss_Reason__c = LossReason)


##UPGRADES##
SeatUpgrades <- fread(file = '/Users/marino/Downloads/Seat Upgrade.csv')
colnames(SeatUpgrades)[3] <- "Email"
expr_Id3 <- paste0("('", paste0(SeatUpgrades$Email[1:nrow(SeatUpgrades)], collapse = "','"),"')")

SeatUpgradeAccounts <- sf_query(paste0("Select Id, PersonEmail from Account where PersonEmail in ",expr_Id3))
expr_Id4 <- paste0("('", paste0(SeatUpgradeAccounts$Id[1:nrow(SeatUpgradeAccounts)], collapse = "','"),"')")

SeatUpgradeOpps <- sf_query(paste0("SELECT Id, AccountId, Amount, Number_of_Tickets__c, StageName, AddOnRequested__c, UpgradeRequested__c FROM Opportunity where CampaignId in ('701UP00000G2uhdYAB', '701UP00000AOwgDYAT') and AccountId in ",expr_Id4))
SeatUpgradeOpps <- SeatUpgradeOpps %>% filter(UpgradeRequested__c != TRUE)
SeatUpgradeOpps$UpgradeRequested__c <- TRUE

##ADD-ONS##
AddOns <- fread(file = '/Users/marino/Downloads/Add Ons.csv')
colnames(AddOns)[3] <- "Email"
expr_Id5 <- paste0("('", paste0(AddOns$Email[1:nrow(AddOns)], collapse = "','"),"')")

AddOnAccounts <- sf_query(paste0("Select Id, PersonEmail from Account where PersonEmail in ",expr_Id5))
expr_Id6 <- paste0("('", paste0(AddOnAccounts$Id[1:nrow(AddOnAccounts)], collapse = "','"),"')")

AddOnOpps <- sf_query(paste0("SELECT Id, AccountId, Amount, Number_of_Tickets__c, StageName, AddOnRequested__c, UpgradeRequested__c FROM Opportunity where CampaignId in ('701UP00000G2uhdYAB', '701UP00000AOwgDYAT') and AccountId in ",expr_Id6))
AddOnOpps <- AddOnOpps %>% filter(AddOnRequested__c != TRUE)
AddOnOpps$AddOnRequested__c <- TRUE

AddOnOpps <- AddOnOpps %>% select(-UpgradeRequested__c)

##TAKE CARE OF ANYONE WHO DID BOTH##
SeatUpgradeOpps2 <- left_join(SeatUpgradeOpps, AddOnOpps, by=c("Id"))
SeatUpgradeOpps2 <- SeatUpgradeOpps2 %>% select(Id, AccountId.x, Amount.x, Number_of_Tickets__c.x, StageName.x, UpgradeRequested__c, AddOnRequested__c.y)
SeatUpgradeOpps2$AddOnRequested__c.y[is.na(SeatUpgradeOpps2$AddOnRequested__c.y)] <- FALSE
colnames(SeatUpgradeOpps2)[2] <- "AccountId"
colnames(SeatUpgradeOpps2)[3] <- "Amount"
colnames(SeatUpgradeOpps2)[4] <- "Number_of_Tickets__c"
colnames(SeatUpgradeOpps2)[5] <- "StageName"
colnames(SeatUpgradeOpps2)[6] <- "UpgradeRequested__c"
colnames(SeatUpgradeOpps2)[7] <- "AddOnRequested__c"
SeatUpgradeOpps2$Loss_Reason__c <- NA
SeatUpgradeOpps2$Description <- NA


AddOnOpps <- sf_query(paste0("SELECT Id, AccountId, Amount, Number_of_Tickets__c, StageName, AddOnRequested__c, UpgradeRequested__c FROM Opportunity where CampaignId in ('701UP00000G2uhdYAB', '701UP00000AOwgDYAT') and AccountId in ",expr_Id6))
AddOnOpps <- AddOnOpps %>% filter(AddOnRequested__c != TRUE)
AddOnOpps$AddOnRequested__c <- TRUE
AddOnOpps2 <- AddOnOpps %>% filter(!AddOnOpps$Id %in% SeatUpgradeOpps2$Id)
AddOnOpps2$Loss_Reason__c <- NA
AddOnOpps2$Description <- NA

Final <- rbind(OptOutOpps, AddOnOpps2, SeatUpgradeOpps2)


result <- sf_update(Final, object_name = "Opportunity")






