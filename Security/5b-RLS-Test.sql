use [TestDb];
go

-- All rows when I am logged in as a db_owner user
revert;  
select * from data.Orders;


-- All rows as Windows user PZVMW16SQL16\WinOpsMgr1 is in Windows group WinOpsMgrs, which is in SQL role OpsMgrRole, which is approved in RLS security predicate function for this table
revert;  
execute as user = 'PZVMW16SQL16\WinOpsMgr1';  
select * from data.Orders;

-- Zero rows, as this Windows user is not a member of any of the "see it all" database roles in the RLS security predicate function for this table
revert;  
execute as user = 'PZVMW16SQL16\WinOpsPerson1';  
select * from data.Orders;

-- All rows as SqlSalesMgr1 is in SalesMgrRole, which is auto-approved in RLS security predicate function for this table
revert;  
execute as user = 'SqlSalesMgr1';  
select * from data.Orders;

-- Only order rows associated with SqlSalesPerson1 (see SalesRep column)
revert;  
execute as user = 'SqlSalesPerson1';  
select * from data.Orders;

-- Only order rows associated with SqlSalesPerson3 (see SalesRep column)
revert;  
execute as user = 'SqlSalesPerson3';  
select * from data.Orders;

-- All rows as SqlSupportMgr1 is in SupportMgrRole, which is auto-approved in RLS security predicate function for this table
revert;  
execute as user = 'SqlSupportMgr1';  
select * from data.Orders;

-- Zero rows, as SqlSupportPerson1 is not a member of any of the "see it all" database roles in the RLS security predicate function for this table
revert;  
execute as user = 'SqlSupportPerson1';  
select * from data.Orders;
