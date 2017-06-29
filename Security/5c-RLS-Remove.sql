use [TestDb];
go

revert;
go

alter security policy Orders_Sales_Filter
with (state = off);
go

drop security policy if exists Orders_Sales_Filter;
go

drop function if exists rls.fn_SecurityPredicate_Orders_Sales;
go

drop schema if exists [rls];
go
