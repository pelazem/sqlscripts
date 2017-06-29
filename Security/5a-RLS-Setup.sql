use [TestDb];
go

create schema [rls];
go

create function rls.fn_SecurityPredicate_Orders_Sales(@SalesRep as sysname)  
	returns table
	with schemabinding
as
    return
	select	1
	as		fn_SecurityPredicate_Orders_Sales_Result   
	where
		@SalesRep = user_name()
		or
		user_name() in ('sa', 'dbo')
		or
		is_member('SalesMgrRole') = 1
		or
		is_member('SupportMgrRole') = 1
		or
		is_member('OpsMgrRole') = 1
	;
go

create security policy
	Orders_Sales_Filter
add filter predicate
	rls.fn_SecurityPredicate_Orders_Sales(SalesRep)
on
	data.Orders
with
	(state = on)
;
go

