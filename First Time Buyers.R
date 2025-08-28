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

con <- dbConnect(RPostgres::Postgres(),
                 dbname = "fcc",
                 host = "db-instance-0.cufbqcappuyq.us-east-1.rds.amazonaws.com",
                 user = "fccreadonly",
                 password = "rI5RC85nRjaabucO@^3%urgH@s6^74%TapKZ!wgs")


report <- dbGetQuery(con, "select ot.crm_id, c.email, c.fname, c.lname_or_account_name, count(*)
from dbt_reporting.ringside_original_ticket ot
left join dbt_reporting.ringside_active_tickets t on ot.crm_id = t.crm_id and ot.section = t.section and ot.row = t.row and ot.seat = t.seat
left join ringside.clients c on ot.crm_id::text = c.crm_id
where ot.event_date > '2021-01-01'
group by ot.crm_id, c.email, c.fname, c.lname_or_account_name
having min(ot.event_date) = '2025-07-31 19:00:00.000000' and min(t.event_date) = '2025-07-31 19:00:00.000000'
;")

#report2 <- dbGetQuery(con, "select crm_id, event_date, event_name, raw_ticket_id, area, paid, price_type from report.ticket t where event_name in ('250405NER')")
report2 <- dbGetQuery(con, "select crm_id, event_date, event_name, raw_ticket_id, area, row, seat, paid, price_type from report.ticket t where is_comp_ticket = FALSE and price_type in ('Adult (Adult)', 'Family Pack', 'PS - You Pick Plan', '10% off', '20% off') and event_name in ('250731MON')")
report3 <- filter(report, (crm_id %in% report2$crm_id))
report3 <- distinct(report3)
report3 <- report3 %>% left_join(report2, by = "crm_id")

    
report4 <- report3 %>% group_by(crm_id, area, row) %>%    
    summarise(
      SeatRange = ifelse(min(seat) == max(seat), 
                         as.character(min(seat)), 
                         paste(min(seat), max(seat), sep = "-")),
      .groups = "drop"
    ) %>%
      mutate(SeatLocation = paste("Section", area, ", Row",row, ", Seat(s)",SeatRange)) %>%
      select(crm_id, SeatLocation) %>%
      group_by(crm_id) %>%
      summarise(SeatLocation = paste(SeatLocation, collapse = ","), .groups = "drop")

report3 <- report3 %>% select(-row)
report3 <- report3 %>% select(-seat)
report3 <- report3 %>% select(-raw_ticket_id)
report3 <- distinct(report3)

report3 <- left_join(report3, report4, by = c("crm_id"))

query <- sf_query("SELECT Primary_Ticketing_ID__pc FROM Account where Primary_Ticketing_ID__pc not in ('') and Full_Season_Ticket_Holder__pc = true")
query2 <- sf_query("SELECT Primary_Ticketing_ID__pc FROM Account where Primary_Ticketing_ID__pc not in ('') and Partial_Season_Ticket_Holder__pc = true")
query3 <- sf_query("SELECT Primary_Ticketing_ID__pc FROM Account where Primary_Ticketing_ID__pc not in ('') and Waitlist_Priority_Rank__pc not in ('')")
query4 <- sf_query("SELECT Primary_Ticketing_ID__pc FROM Account where Primary_Ticketing_ID__pc not in ('') and Ticketing_Service_Rep__pc not in ('')")

query4 <- rbind(query, query2, query3, query4)
report3 <- filter(report3, !(crm_id %in% query4$Primary_Ticketing_ID__pc))
report4 <- data.frame(report3$crm_id)
report4 <- distinct(report4)

expr_Id1 <- paste0("('", paste0(report4$report3.crm_id[1:nrow(report4)], collapse = "','"),"')")
query11 <- sf_query(paste0("SELECT Primary_Ticketing_ID__pc, ID, X2024_Events_Attended__pc, X2025_Events_Attended__pc from Account where Primary_Ticketing_ID__pc in ",expr_Id1))
query11$Primary_Ticketing_ID__pc <- as.integer(query11$Primary_Ticketing_ID__pc)
query11 <- left_join(query11, report3, by=c('Primary_Ticketing_ID__pc' = 'crm_id'))
query11 <- query11 %>% group_by(Id) %>% mutate(spend = sum(paid, na.rm = TRUE)) %>% ungroup()
#query11 <- subset(query11, select = -raw_ticket_id)
query11 <- subset(query11, select = -paid)
query11 <- distinct(query11)

query12 <- sf_query("Select AccountId, CampaignId from Opportunity where CampaignId in ('701UP00000K3LSeYAN')")
query11 <- filter(query11, !(Id %in% query12$AccountId))


query11$totalspend <- query11$count * query11$spend
query11$priority <- ifelse(query11$totalspend < 250, "c_Cold",
                         ifelse(query11$totalspend >= 250 & query11$totalspend <= 500, "b_Warm", 
                                "a_Hot"))

query13 <- filter(query11, is.na(query11$X2024_Events_Attended__pc))
query13 <- filter(query13, is.na(query13$X2025_Events_Attended__pc))
query11 <- query13

query11 <- query11 %>%
  group_by(Primary_Ticketing_ID__pc) %>% 
  slice_head(n = 1) %>% 
  ungroup()

Owners <- sf_query("Select Name, Id from User where IsActive = TRUE")
reps <- data.frame("Name" = c('Undistributed Lead'))
reps <- left_join(reps, Owners, by = c('Name' = 'Name'))
reps <-  cbind(reps, rep(row.names(reps), each = nrow(query11)))

CreateOppswOwners <-data.frame("AccountId" = query11$Id, 
                               "Name" = "Ticketing Opp Upload", 
                               "agilitek__Start_Season__c" = "a1d8Z000004gVxWQAU",
                               "CloseDate" = paste0(today+30,"T00:00:00Z"),
                               "CampaignId" = "701UP00000K3LSeYAN",
                               "Follow_Up_By__c" = paste0(today, "T00:00:00Z"),
                               "NextStep" = "Initial Phone Call",
                               "StageName" = "No Contact",
                               "OwnerId" = reps$Id,
                               "Type" = "New",
                               "LeadSource" = "Previous Buyer",
                               "Number_of_Tickets__c" = 0,
                               "Amount" = 0,
                               "RecordTypeId" = "0128Z0000002EsaQAE",
                               "Follow_Up_Priority__c" = query11$priority,
                               "Description" = paste0("First Time Buyer, attending 7/31 Match vs Monterrey. Sitting in ", query11$SeatLocation," and paid $",query11$spend)
)

CreateOppswOwners <- distinct(CreateOppswOwners)

result <- sf_create(CreateOppswOwners, object_name = "Opportunity")
