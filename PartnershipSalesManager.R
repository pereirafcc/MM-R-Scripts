##ACTIVITY TRACKER##
today = Sys.Date()
lastweek = today - 7
tasksthisweek <- sf_query(paste0("SELECT OwnerId, Type, AccountId, CompletedDateTime, Subject FROM Task where OwnerId in ('0058Z00000AeGGKQA3', '0051U000005rhR2QAI', '005UP000001IS0WYAW') and Status in ('Completed') and CompletedDateTime > ",lastweek,"T00:00:00Z"))
tasksthisweek <- tasksthisweek[!is.na(tasksthisweek$AccountId), ]
expr_Id01 <- paste0("('", paste0(tasksthisweek$AccountId[1:nrow(tasksthisweek)], collapse = "','"),"')")
contactaccounts <- sf_query(paste0("Select Id, Name from Account where Id in",expr_Id01))
tasksthisweek <- left_join(tasksthisweek, contactaccounts, by=c("AccountId" = "Id"))
tasksthisweek$OwnerId[tasksthisweek$OwnerId == "0058Z00000AeGGKQA3"] <- "Joe Thomas"
tasksthisweek$OwnerId[tasksthisweek$OwnerId == "0051U000005rhR2QAI"] <- "Matt Shearer"
tasksthisweek$OwnerId[tasksthisweek$OwnerId == "005UP000001IS0WYAW"] <- "Steven Young"
fwrite(tasksthisweek, file = '/Users/marino/Desktop/Sales Manager Dashboard/tasksthisweek.csv')


##ACCOUNTS IN PIPELINE##
last90 = today - 90
pipelineaccounts <- sf_query(paste0("SELECT Name, LastActivityDate, Days_Since_Last_Activity__c, Partnership_Sales_Rep__c FROM Account where Partnership_Sales_Rep__c in ('0058Z00000AeGGKQA3', '0051U000005rhR2QAI') and Current_Partner__c = FALSE and X2025_Partnership__c = FALSE and LastActivityDate > ",last90))
pipelineaccounts$Partnership_Sales_Rep__c[pipelineaccounts$Partnership_Sales_Rep__c == "0058Z00000AeGGKQA3"] <- "Joe Thomas"
pipelineaccounts$Partnership_Sales_Rep__c[pipelineaccounts$Partnership_Sales_Rep__c == "0051U000005rhR2QAI"] <- "Matt Shearer"
pipelineaccounts$Partnership_Sales_Rep__c[pipelineaccounts$Partnership_Sales_Rep__c == "005UP000001IS0WYAW"] <- "Steven Young"
fwrite(pipelineaccounts, file = '/Users/marino/Desktop/Sales Manager Dashboard/pipelineaccounts.csv')


#PIPELINE BY AGREEMENT STAGE##
#REVENUE BY AGREEMENT STAGE##
pbas <- sf_query("SELECT OwnerId, agilitek__Account__c, agilitek__Stage__c, agilitek__Start_Season__c, agilitek__Contract_Length_Years__c, agilitek__Total_Deal_Value__c FROM agilitek__Agreement__c where agilitek__Stage__c not in ('Fully Executed', 'Closed Lost') and OwnerId in ('0058Z00000AeGGKQA3', '0051U000005rhR2QAI', '005UP000001IS0WYAW') and agilitek__Type__c in ('New')")
expr_Id02 <- paste0("('", paste0(pbas$agilitek__Account__c[1:nrow(pbas)], collapse = "','"),"')")
contactaccounts <- sf_query(paste0("Select Id, Name from Account where Id in",expr_Id02))
pbas <- left_join(pbas, contactaccounts, by=c("agilitek__Account__c" = "Id"))
pbas$OwnerId[pbas$OwnerId == "0058Z00000AeGGKQA3"] <- "Joe Thomas"
pbas$OwnerId[pbas$OwnerId == "0051U000005rhR2QAI"] <- "Matt Shearer"
pbas$OwnerId[pbas$OwnerId == "005UP000001IS0WYAW"] <- "Steven Young"
fwrite(pbas, file = '/Users/marino/Desktop/Sales Manager Dashboard/agreementstage.csv')

##LIVE ASSET PITCHES##
opps <- sf_query("SELECT AccountId, Account_Name_Slack__c, StageName, agilitek__Contract_Length_in_Years__c, agilitek__Deal_Value__c, OwnerId, CloseDate FROM Opportunity where agilitek__Start_Season__c in ('a1d8Z000004gVxWQAU', 'a1d8Z000004gVxgQAE') and agilitek__Deal_Value__c > 0 and OwnerId in ('0058Z00000AeGGKQA3', '0051U000005rhR2QAI', '005UP000001IS0WYAW')")
expr_Id03 <- paste0("('", paste0(opps$AccountId[1:nrow(pbas)], collapse = "','"),"')")
agrs <- sf_query("SELECT agilitek__Account__c, agilitek__Start_Season__c, agilitek__Stage__c, agilitek__Type__c FROM agilitek__Agreement__c")
laps <- left_join(opps, agrs, by = c("AccountId" = "agilitek__Account__c"))
laps <- laps %>% filter(agilitek__Stage__c != "Closed Lost")
laps <- laps %>% filter(agilitek__Stage__c != "Fully Executed")
laps <- laps %>% filter(agilitek__Type__c == "New")
laps$OwnerId[laps$OwnerId == "0058Z00000AeGGKQA3"] <- "Joe Thomas"
laps$OwnerId[laps$OwnerId == "0051U000005rhR2QAI"] <- "Matt Shearer"
laps$OwnerId[laps$OwnerId == "005UP000001IS0WYAW"] <- "Steven Young"
laps$agilitek__Start_Season__c[laps$agilitek__Start_Season__c == "a1d8Z000004gVxWQAU"] <- 2025
laps$agilitek__Start_Season__c[laps$agilitek__Start_Season__c == "a1d8Z000004gVxgQAE"] <- 2026
laps <- laps %>% filter(StageName != "Closed Lost")
laps <- laps %>% filter(StageName != "Fully Executed")
laps <- laps %>% filter(StageName != "First Conversation")
laps <- laps %>%
  group_by(AccountId) %>%
  slice_min(order_by = agilitek__Deal_Value__c, with_ties = FALSE) %>%
  ungroup()
expr_Id06 <- paste0("('", paste0(laps$AccountId[1:nrow(laps)], collapse = "','"),"')")
lastcontacted <- sf_query(paste0("Select Id, Last_Contact_Date__c from Account where Id in ",expr_Id06))
laps <- left_join(laps,lastcontacted, by=c("AccountId"="Id"))
laps$agilitek__Deal_Value__c <- as.numeric(laps$agilitek__Deal_Value__c)
fwrite(laps, file = '/Users/marino/Desktop/Sales Manager Dashboard/assetpitches.csv')


##REVENUE TRACKER##
fullyexecuted <- sf_query("SELECT Id, agilitek__Account__c FROM agilitek__Agreement__c where agilitek__Stage__c in ('Fully Executed')")
expr_Id04 <- paste0("('", paste0(fullyexecuted$Id[1:nrow(fullyexecuted)], collapse = "','"),"')")
feopps <- sf_query(paste0("Select Account_Name_Slack__c, agilitek__Deal_Value__c, agilitek__Start_Season__c from Opportunity where agilitek__Agreement__c in ",expr_Id04))
startseasons <- sf_query("Select Name, Id from agilitek__Season__c")
feopps <- left_join(feopps, startseasons, by=c("agilitek__Start_Season__c" = "Id"))
fwrite(feopps, file = '/Users/marino/Desktop/Sales Manager Dashboard/revenuetracker.csv')


##2025 INVENTORY AVAILABLE##
##ASSETS BEING PITCHED##
inventory <- sf_query("SELECT Id, agilitek__Product_Family__c, agilitek__Product_Sub_Family__c, agilitek__Product_Name__c, agilitek__Quantity_Total__c, agilitek__Quantity_Sold__c, agilitek__Quantity_Pitched__c, agilitek__Quantity_Available__c FROM agilitek__Inventory_by_Season__c where agilitek__Season__c in ('a1d8Z000004gVxWQAU') and agilitek__Active__c = TRUE")
inventory[is.na(inventory)] <- 0
expr_Id05 <- paste0("('", paste0(inventory$Id[1:nrow(inventory)], collapse = "','"),"')")
rates <- sf_query(paste0("SELECT agilitek__InventoryBySeasonID__c, agilitek__Rate__c FROM agilitek__Rate__c where agilitek__InventoryBySeasonID__c in ",expr_Id05))
inventory <- left_join(inventory, rates, by=c("Id" = "agilitek__InventoryBySeasonID__c"))
fwrite(inventory, file = '/Users/marino/Desktop/Sales Manager Dashboard/inventory.csv')


##2025 RENEWAL DASHBOARD##
availrenewals <- sf_query("SELECT agilitek__Account__c, Id, Name FROM agilitek__Agreement__c where agilitek__Stage__c in ('Fully Executed') and agilitek__Start_Season__c in ('a1d8Z000004gVxWQAU') and agilitek__Contract_Length_Years__c = 1")
expr_Id07 <- paste0("('", paste0(availrenewals$agilitek__Account__c[1:nrow(availrenewals)], collapse = "','"),"')")
fullyexecuted <- sf_query(paste0("Select Account_Name_Slack__c, AccountId, agilitek__Deal_Value__c from Opportunity where agilitek__Start_Season__c in ('a1d8Z000004gVxWQAU') and StageName in ('Fully Executed') and AccountId in ",expr_Id07))
fullyexecuted <- setNames(fullyexecuted, c("AccountName", "AccountId", "2025 Deal Value"))
actrenewals <- sf_query("SELECT agilitek__Account__c, Id, Name, agilitek__Start_Season__c, agilitek__Stage__c, agilitek__Type__c FROM agilitek__Agreement__c where agilitek__Type__c in ('Renewal') and agilitek__Start_Season__c in ('a1d8Z000004gVxgQAE')")
fullrenewals <- left_join(availrenewals, actrenewals, by=c("agilitek__Account__c" = "agilitek__Account__c"))
expr_Id05 <- paste0("('", paste0(actrenewals$Id[1:nrow(actrenewals)], collapse = "','"),"')")
renewalopps <- sf_query(paste0("Select AccountId, Account_Name_Slack__c, agilitek__Deal_Value__c, agilitek__Start_Season__c, agilitek__Percent_of_Rate__c from Opportunity where agilitek__Start_Season__c in ('a1d8Z000004gVxgQAE') and agilitek__Agreement__c in ",expr_Id05))
renewaldash <- left_join(fullrenewals, renewalopps, by=c("agilitek__Account__c" = "AccountId"))
renewaldash <- left_join(renewaldash, fullyexecuted, by=c("agilitek__Account__c" = "AccountId"))
renewaldash$agilitek__Percent_of_Rate__c <- renewaldash$agilitek__Percent_of_Rate__c/100
renewaldash$agilitek__Stage__c[is.na(renewaldash$agilitek__Stage__c)] <- "Not Created"
renewaldash$agilitek__Deal_Value__c[is.na(renewaldash$agilitek__Deal_Value__c)] <- 0
renewaldash$agilitek__Percent_of_Rate__c[is.na(renewaldash$agilitek__Percent_of_Rate__c)] <- 0
#renewaldash <- renewaldash[order(-renewaldash$agilitek__Deal_Value__c), !duplicated(renewaldash$AccountName), ]
renewaldash <- renewaldash %>%
  arrange(desc(agilitek__Deal_Value__c)) %>%
  distinct(AccountName, .keep_all = TRUE)
fwrite(renewaldash, file = '/Users/marino/Desktop/Sales Manager Dashboard/renewals.csv')






