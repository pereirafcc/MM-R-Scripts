library(data.table)
sf_auth()


#PULL ALL JOE OPPS SINCE 7/1##
query <- sf_query("Select Id, AccountId, Product__c, Follow_Up_By__c, CloseDate from Opportunity where OwnerId in ('0058Z000009eIjdQAE') and CreatedDate > 2024-07-01T00:00:00Z and CampaignId in ('701UP000009nAibYAE')")

#PULL ALL NON-JOE 2025 WEB LEADS##
query6 <- sf_query("Select Id, AccountId, Product__c, Follow_Up_By__c, CloseDate from Opportunity where OwnerId not in ('0058Z000009eIjdQAE') and CampaignId in ('701UP000009nAibYAE')")

#PULL ALL NON-JOE 2024 WEB LEADS IN LAST 30 DAYS##
query7 <- sf_query("Select Id, AccountId, Product__c, Follow_Up_By__c, CloseDate from Opportunity where OwnerId not in ('0058Z000009eIjdQAE') and CampaignId in ('7018Z000003N9g7QAC') and CreatedDate > 2024-07-01T00:00:00Z")

#PULL ANY ACCOUNTS WITH OPEN 2025 OPP#
query8 <- sf_query("Select Id, AccountId, Product__c, Follow_Up_By__c, CloseDate from Opportunity where OwnerId not in ('0058Z000009eIjdQAE') and Start_Season_Name__c in ('2025')")


#PULL IN ANY OBR ACCOUNTS##
#query9 <- sf_query("SELECT Id FROM Account where Waitlist_Priority_Rank__pc not in ('')")


#SCRUB ANY RECENT OPEN WEB LEADS##
query <- filter(query, !(query$AccountId %in% query6$AccountId))
query <- filter(query, !(query$AccountId %in% query7$AccountId))
query <- filter(query, !(query$AccountId %in% query8$AccountId))
#query <- filter(query, !(query$AccountId %in% query9$Id))


#PULL IN SERVICE REP AND LAST CONTACT#
expr_Id<- paste0("('", paste0(query$AccountId[1:nrow(query)], collapse = "','"),"')")

query2 <- sf_query(paste0("Select Id, Ticketing_Service_Rep__pc, LastActivityDate, Last_Contacted_By__c from Account where Id in ", expr_Id))
if(!("Ticketing_Service_Rep__pc" %in% colnames(query2)))
  {
  query2$Ticketing_Service_Rep__pc <- ''
}

if(!("Last_Contacted_By__c" %in% colnames(query2)))
{
  query2$Last_Contacted_By__c <- ''
}

if(!("LastActivityDate" %in% colnames(query2)))
{
  query2$LastActivityDate <- ''
}

Owners <- sf_query("Select Name, Id from User where IsActive = TRUE")
query2 <- left_join(query2, Owners, by = c('Ticketing_Service_Rep__pc' = 'Id'))
query3 <- left_join(query2, Owners, by = c('Last_Contacted_By__c' = 'Id'))

query <- left_join(query, query3, by = c('AccountId' = 'Id'))
today = Sys.Date()

query$CloseDate <- today+30
query$Follow_Up_By__c <- today+14

#ASSIGN OWNED ACCOUNTS TO THEIR OWNER#
query$Owner <- ifelse(is.na(query$Name.x) & query$LastActivityDate > today - 30 ,query$Name.y,query$Name.x)
OwnedOpps <- filter(query, !is.na(query$Owner))
OwnedOpps <- OwnedOpps[c("Id", "Owner")]
if(nrow(OwnedOpps) > 0)
{
OwnedOpps <- left_join(OwnedOpps, Owners, by = c('Owner' = 'Name'))
colnames(OwnedOpps) <- c("Id", "OwnerName", "OwnerId")
OwnedOpps <- OwnedOpps[c("Id", "OwnerId")]
}

UnownedOpps <- filter(query, is.na(query$Owner))
UnownedOpps <- UnownedOpps[c("Id", "Product__c")]

WaitlistReps <- data.frame("Name" = c('Ian McCarthy', 'Hannah Clendenin', 'Olivia Craddock', 'Toby Varland', 'Shawn Mather', 'Cassie Collins', 'Kayla Reed', 'Zack Lutz', 'Will Catlett'))
WaitlistReps <- left_join(WaitlistReps, Owners, by = c('Name' = 'Name'))
WaitlistReps <- cbind(WaitlistReps, rep(row.names(WaitlistReps), each = 10))

GroupsReps <-  data.frame("Name" = c('Gerardo Garcia Pantoja', 'Brandon Jones'))
GroupsReps <- left_join(GroupsReps, Owners, by = c('Name' = 'Name'))
GroupsReps <- cbind(GroupsReps, rep(row.names(GroupsReps), each = 10))

PremiumReps <- data.frame("Name" = c('Matt Mulvanny', 'Dominic Zahn', 'Jimmy Dollard', 'Tori Priest'))
PremiumReps <- left_join(PremiumReps, Owners, by = c('Name' = 'Name'))
PremiumReps <- cbind(PremiumReps, rep(row.names(PremiumReps), each = 10))

UnownedWaitlist <- filter(UnownedOpps, Product__c == "Waitlist")
WaitlistReps <- WaitlistReps[1:(nrow(UnownedWaitlist)),]
UnownedWaitlist$Owner <- WaitlistReps$Id
colnames(UnownedWaitlist) <- c("Id", "OwnerName", "OwnerId")
UnownedWaitlist <- UnownedWaitlist[c("Id", "OwnerId")]

UnownedGroups <- filter(UnownedOpps, Product__c == "Group")
GroupsReps <- GroupsReps[1:(nrow(UnownedGroups)),]
UnownedGroups$Owner <- GroupsReps$Id
colnames(UnownedGroups) <- c("Id", "OwnerName", "OwnerId")
UnownedGroups <- UnownedGroups[c("Id", "OwnerId")]

UnownedDeposits <- filter(UnownedOpps, Product__c == "Deposit")
DepositReps <- GroupsReps[1:(nrow(UnownedDeposits)),]
UnownedDeposits$Owner <- DepositReps$Id
colnames(UnownedDeposits) <- c("Id", "OwnerName", "OwnerId")
UnownedDeposits <- UnownedDeposits[c("Id", "OwnerId")]

UnownedPremium <- filter(UnownedOpps, Product__c == "Premium Single Deposit")
PremiumReps <- PremiumReps[1:(nrow(UnownedPremium)),]
UnownedPremium$Owner <- PremiumReps$Id
colnames(UnownedPremium) <- c("Id", "OwnerName", "OwnerId")
UnownedPremium <- UnownedPremium[c("Id", "OwnerId")]

Distribution <- rbind(UnownedWaitlist, UnownedGroups, UnownedPremium, UnownedDeposits, OwnedOpps)
fwrite(Distribution, file = '/Users/marino/Desktop/WebLeadDistribution.csv')
  
  
  