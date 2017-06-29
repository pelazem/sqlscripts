use [TestDb];
go

-- ==========
-- MASKED

-- ----------
-- Begin Windows users and groups
/**
Windows users and groups: substitute your domain or server name below. Note, you cannot use UPN notation here, must use domain\username or server\username.
Windows users WinOpsMgr1, WinOpsPerson1, WinOpsPerson2. These users were NOT explicitly added to SQL Server, or to the database.
Windows user WinOpsMgr1 is a member of Windows group WinOpsMgrs. Windows group WinOpsMgrs was added to the SQL database role OpsMgrRole.
Windows users WinOpsMgr1, WinOpsPerson1 and WinOpsPerson2 are members of Windows group WinOpsPersons. Windows group WinOpsPersons was added to the SQL database role OpsPersonRole.
So executing as these Windows users works through Windows groups and SQL database roles, which means users can be managed in AD as usual
without further per-user management in SQL Server to take advantage of DDM.
**/
-- Here we expect MASKED - i.e. obscured - since PZVMW16SQL16\WinOpsMgr1 is in OpsPersonRole and OpsMgrRole, which by default are DDM masked.
revert;  
execute as user = 'PZVMW16SQL16\WinOpsMgr1';  
select * from data.Users;
select * from data.Orders;
exec data.GetUsers;
exec data.GetOrders;

-- Now we will grant UNMASK to OpsMgrRole and execute identically to previous, to show how to grant certain roles to see data in the clear.
-- We expect clear data since PZVMW16SQL16\WinOpsMgr1 is in both OpsMgrRole and OpsPersonRole, and OpsMgrRole is granted DDM unmask
revert;
grant unmask to [OpsMgrRole];
execute as user = 'PZVMW16SQL16\WinOpsMgr1';  
select * from data.Users;
select * from data.Orders;
exec data.GetUsers;
exec data.GetOrders;
-- We can revoke DDM unmask from OpsMgrRole again; then PZVMW16SQL16\WinOpsMgr1 (or anyone else in that role) will see masked data again
revert;
revoke unmask to [OpsMgrRole];


-- Here we expect MASKED - i.e. obscured - since WinOpsPersons is in OpsPersonRole, which by default is DDM masked.
revert;  
execute as user = 'PZVMW16SQL16\WinOpsPerson1';  
select * from data.Users;
select * from data.Orders;
exec data.GetUsers;
exec data.GetOrders;

-- Here we expect MASKED - i.e. obscured - since WinOpsPersons is in OpsPersonRole, which by default is DDM masked.
revert;  
execute as user = 'PZVMW16SQL16\WinOpsPerson2';  
select * from data.Users;
select * from data.Orders;
exec data.GetUsers;
exec data.GetOrders;
-- End Windows users and groups
-- ----------

-- ----------
-- Begin SQL users and groups

-- Here we expect MASKED - i.e. obscured - since SqlSalesMgr1 is in SalesPersonRole and SalesMgrRole, which by default are DDM masked.
revert;  
execute as user = 'SqlSalesMgr1';  
select * from data.Users;
select * from data.Orders;
exec data.GetUsers;
exec data.GetOrders;

-- Now we will grant UNMASK to SalesMgrRole and execute identically to previous, to show how to grant certain roles to see data in the clear.
-- We expect clear data since SqlSalesMgr1 is in both SalesMgrRole and SalesPersonRole, and SalesMgrRole is granted DDM unmask
revert;
grant unmask to [SalesMgrRole];
execute as user = 'SqlSalesMgr1';  
select * from data.Users;
select * from data.Orders;
exec data.GetUsers;
exec data.GetOrders;
-- We can revoke DDM unmask from SalesMgrRole again; then SqlSalesMgr1 (or anyone else in that role) will see masked data again
revert;
revoke unmask to [SalesMgrRole];


-- Here we expect MASKED - i.e. obscured - since SqlSalesPersonX is in SalesPersonRole, which by default is DDM masked.
revert;  
execute as user = 'SqlSalesPerson1';  
select * from data.Users;
select * from data.Orders;
exec data.GetUsers;
exec data.GetOrders;

revert;  
execute as user = 'SqlSalesPerson3';  
select * from data.Users;
select * from data.Orders;
exec data.GetUsers;
exec data.GetOrders;



-- Here we expect MASKED - i.e. obscured - since SqlSupportMgr1 is in SupportMgrRole and SupportPersonRole, which by default are DDM masked.
revert;  
execute as user = 'SqlSupportMgr1';  
select * from data.Users;
select * from data.Orders;
exec data.GetUsers;
exec data.GetOrders;

-- Now we will grant UNMASK to SupportMgrRole and execute identically to previous, to show how to grant certain roles to see data in the clear.
-- We expect clear data since SqlSupportMgr1 is in both SupportMgrRole and SupportPersonRole, and SupportMgrRole is granted DDM unmask
revert;
grant unmask to [SupportMgrRole];
execute as user = 'SqlSupportMgr1';  
select * from data.Users;
select * from data.Orders;
exec data.GetUsers;
exec data.GetOrders;
-- We can revoke DDM unmask from SupportMgrRole again; then SqlSupportMgr1 (or anyone else in that role) will see masked data again
revert;
revoke unmask to [SupportMgrRole];


-- Here we expect MASKED - i.e. obscured - since SqlSupportPersonX is in SupportPersonRole, which by default is DDM masked.
revert;  
execute as user = 'SqlSupportPerson1';  
select * from data.Users;
select * from data.Orders;
exec data.GetUsers;
exec data.GetOrders;

revert;  
execute as user = 'SqlSupportPerson3';  
select * from data.Users;
select * from data.Orders;
exec data.GetUsers;
exec data.GetOrders;

-- End SQL users and groups
-- ----------

-- ==========

-- ==========
-- Revert to my normal login = UNMASKED
revert;  

select * from data.Users;
select * from data.Orders;
exec data.GetUsers;
exec data.GetOrders;
-- ==========

