
wonopps <- sf_query("SELECT OwnerId, Amount, CloseDate, Product__c FROM Opportunity where StageName in ('Closed Won') and Type in ('New') and CloseDate > 2024-12-31")

Owners <- sf_query("Select Name, Id from User where IsActive = TRUE")
wonopps <- left_join(wonopps, Owners, by = c('OwnerId' = 'Id'))

fwrite(wonopps, file = '/Users/marino/Desktop/monthlyrevenue.csv')