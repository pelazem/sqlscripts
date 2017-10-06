-- ==================================================
/**
PREREQUISITES

---------------------------
---------------------------
WINDOWS/AD GROUPS AND USERS
---------------------------
This script assumes the following Windows/AD users and groups exist. They do NOT need to be mapped to SQL Server logins; this script handles that for the database.

Group: Test_WinOpsPeople
Members: Test_WinOpsPerson1, Test_WinOpsPerson2

Group: Test_WinOpsMgrs
Members: Test_WinOpsMgr1
---------------------------
---------------------------

**/
-- ==================================================


-- ==================================================
-- BEGIN DATABASE SETUP

USE [master];
GO

sp_configure 'contained database authentication', 1;  
GO  
RECONFIGURE;  
GO

drop database if exists [TestDb];
go

CREATE DATABASE [TestDb]
	CONTAINMENT = NONE
	ON PRIMARY 
( NAME = N'TestDb', FILENAME = N'G:\Program Files\Microsoft SQL Server\MSSQL14.MSSQLSERVER\MSSQL\DATA\TestDb.mdf', SIZE = 128MB , MAXSIZE = UNLIMITED, FILEGROWTH = 128MB )
 LOG ON 
( NAME = N'TestDb_log', FILENAME = N'H:\Program Files\Microsoft SQL Server\MSSQL14.MSSQLSERVER\MSSQL\DATA\TestDb_log.ldf', SIZE = 64MB , MAXSIZE = 2048GB , FILEGROWTH = 64MB )
;
GO

ALTER DATABASE [TestDb] SET COMPATIBILITY_LEVEL = 140
GO

IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
	EXEC [TestDb].[dbo].[sp_fulltext_database] @action = 'enable';
end
GO

ALTER DATABASE [TestDb] SET CONTAINMENT = PARTIAL;
go
ALTER DATABASE [TestDb] SET RECOVERY SIMPLE;
GO
ALTER DATABASE [TestDb] SET  MULTI_USER;
GO
ALTER DATABASE [TestDb] SET READ_WRITE;
GO
ALTER DATABASE [TestDb] SET QUERY_STORE = ON;
GO

-- END DATABASE SETUP
-- ==================================================

USE [TestDb];
go

-- ==================================================
-- BEGIN SCHEMAS
create schema [data];
go
-- END SCHEMAS
-- ==================================================


-- ==================================================
-- BEGIN ROLES

create role [OpsPersonRole];
create role [OpsMgrRole];
create role [SalesPersonRole];
create role [SalesMgrRole];
create role [SupportPersonRole];
create role [SupportMgrRole];
go

-- END ROLES
-- ==================================================


-- ==================================================
-- BEGIN USERS


create user [Test_WinOpsMgrs] for login [NETGAIN\Test_WinOpsMgrs];
create user [Test_WinOpsPeople] for login [NETGAIN\Test_WinOpsPeople];
go

alter role [OpsPersonRole] add member [Test_WinOpsMgrs];
alter role [OpsPersonRole] add member [Test_WinOpsPeople];
go

alter role [OpsMgrRole] add member [Test_WinOpsMgrs];
go


create user SqlSupportMgr1 with password = N'P@ssw0rd2017!', DEFAULT_SCHEMA=[data];
create user SqlSupportPerson1 with password = N'P@ssw0rd2017!', DEFAULT_SCHEMA=[data];
create user SqlSupportPerson2 with password = N'P@ssw0rd2017!', DEFAULT_SCHEMA=[data];
create user SqlSupportPerson3 with password = N'P@ssw0rd2017!', DEFAULT_SCHEMA=[data];
go

alter role [SupportMgrRole] add member [SqlSupportMgr1];
go

alter role [SupportPersonRole] add member [SqlSupportMgr1];
alter role [SupportPersonRole] add member [SqlSupportPerson1];
alter role [SupportPersonRole] add member [SqlSupportPerson2];
alter role [SupportPersonRole] add member [SqlSupportPerson3];
go


create user SqlSalesMgr1 with password = N'P@ssw0rd2017!', DEFAULT_SCHEMA=[data];
create user SqlSalesPerson1 with password = N'P@ssw0rd2017!', DEFAULT_SCHEMA=[data];
create user SqlSalesPerson2 with password = N'P@ssw0rd2017!', DEFAULT_SCHEMA=[data];
create user SqlSalesPerson3 with password = N'P@ssw0rd2017!', DEFAULT_SCHEMA=[data];
create user SqlSalesPerson4 with password = N'P@ssw0rd2017!', DEFAULT_SCHEMA=[data];
create user SqlSalesPerson5 with password = N'P@ssw0rd2017!', DEFAULT_SCHEMA=[data];
go

alter role [SalesMgrRole] add member [SqlSalesMgr1];
go

alter role [SalesPersonRole] add member [SqlSalesMgr1];
alter role [SalesPersonRole] add member [SqlSalesPerson1];
alter role [SalesPersonRole] add member [SqlSalesPerson2];
alter role [SalesPersonRole] add member [SqlSalesPerson3];
alter role [SalesPersonRole] add member [SqlSalesPerson4];
alter role [SalesPersonRole] add member [SqlSalesPerson5];
go

-- END USERS
-- ==================================================


-- ==================================================
-- BEGIN SECURITY
grant execute, select on schema :: [data] to [OpsPersonRole];
grant execute, select on schema :: [data] to [OpsMgrRole];
grant execute, select on schema :: [data] to [SalesPersonRole];
grant execute, select on schema :: [data] to [SalesMgrRole];
grant execute, select on schema :: [data] to [SupportPersonRole];
grant execute, select on schema :: [data] to [SupportMgrRole];
go
-- END SECURITY
-- ==================================================


-- ==================================================
-- BEGIN TABLES

create table [data].[Users]
(
	[UserGuid] [uniqueidentifier] constraint [PK_data_Users_UserGuid] primary key nonclustered not null,
	[UserName] [nvarchar](50) null,
	[FirstName] [nvarchar](50) null,
	[LastName] [nvarchar](50) null,
	[EMail] [nvarchar](50) null,
	[SSN] [nvarchar](20) null,
	[DoB] [datetime2](3) null,
	[DateCreated] [datetime2] null,
	[DateUpdated] [datetime2] null
)
on [primary];
go

alter table [data].[Users] add constraint [DF_data_Users_UserGuid] default (newsequentialid()) for [UserGuid];
alter table [data].[Users] add constraint [DF_data_Users_DateCreated] default (getutcdate()) for [DateCreated];
alter table [data].[Users] add constraint [DF_data_Users_DateUpdated] default (getutcdate()) for [DateUpdated];
go


create table [data].[Orders]
(
	[OrderGuid] [uniqueidentifier] constraint [PK_data_Orders_OrderGuid] primary key nonclustered not null,
	[UserGuid] [uniqueidentifier] null,
	[OrderName] [nvarchar](50) null,
	[OrderDate] [datetime2] null,
	[SalesRep] sysname null,
	[CreditCardNumber] [nvarchar](50) null,
	[CreditCardExp] [nvarchar](20) null,
	[CreditCardSecCode] [nvarchar](10) null,
	[DateCreated] [datetime2] null,
	[DateUpdated] [datetime2] null
)
on [primary];
go

alter table [data].[Orders] add constraint [DF_data_Orders_OrderGuid] default (newsequentialid()) for [OrderGuid];
alter table [data].[Orders] add constraint [DF_data_Orders_OrderDate] default (getutcdate()) for [OrderDate];
alter table [data].[Orders] add constraint [DF_data_Orders_DateCreated] default (getutcdate()) for [DateCreated];
alter table [data].[Orders] add constraint [DF_data_Orders_DateUpdated] default (getutcdate()) for [DateUpdated];
go

create table [data].[OrderLines]
(
	[OrderLineGuid] [uniqueidentifier] constraint [PK_data_OrderLines_OrderLineGuid] primary key nonclustered not null,
	[OrderGuid] [uniqueidentifier] not null,
	[ProductGuid] [uniqueidentifier] not null,
	[ProductQty] [int] null,
	[UnitCost] [float] null,
	[DateCreated] [datetime2] null,
	[DateUpdated] [datetime2] null
)
on [primary];
go

alter table [data].[OrderLines] add constraint [DF_data_OrderLines_OrderLineGuid] default (newsequentialid()) for [OrderLineGuid];
alter table [data].[OrderLines] add constraint [DF_data_OrderLines_DateCreated] default (getutcdate()) for [DateCreated];
alter table [data].[OrderLines] add constraint [DF_data_OrderLines_DateUpdated] default (getutcdate()) for [DateUpdated];
go

-- END TABLES
-- ==================================================



-- ==================================================
-- BEGIN STORED PROCEDURES

create proc [data].[CreateUser]
	@UserName		nvarchar(50),
	@FirstName		nvarchar(50),
	@LastName		nvarchar(50),
	@EMail			nvarchar(50),
	@SSN			nvarchar(20) = null,
	@DoB			datetime2(3) = null,
	@UserGuid		uniqueidentifier = null output
as
begin
	select	@UserGuid = newid();

	insert into data.Users
	(
		UserGuid,
		UserName,
		FirstName,
		LastName,
		EMail,
		SSN,
		DoB
	)
	values
	(
		@UserGuid,
		@UserName,
		@FirstName,
		@LastName,
		@EMail,
		@SSN,
		@DoB
	);
end
go

create proc [data].[DeleteUser]
	@UserGuid		uniqueidentifier
as
begin
	delete from data.Users
	where	[UserGuid] = @UserGuid;
end
go

create proc [data].[UpdateUser]
	@UserGuid		uniqueidentifier,
	@EMail			nvarchar(50),
	@SSN			nvarchar(20),
	@DoB			datetime2(3)
as
begin
	update
		[data].[Users]
	set
		[EMail] = @EMail,
		[SSN] = @SSN,
		[DoB] = @DoB,
		[DateUpdated] = getutcdate()
	where
		[UserGuid] = @UserGuid
	;
end
go

create proc [data].[GetUsers]
as
begin
	select
		UserGuid,
		UserName,
		FirstName,
		LastName,
		EMail,
		SSN,
		DoB,
		DateCreated,
		DateUpdated
	from
		data.Users;
end
go

create proc [data].[CreateOrder]
	@UserGuid			uniqueidentifier = null,
	@OrderName			nvarchar(50) = null,
	@OrderDate			datetime2 = null,
	@SalesRep			sysname = null,
	@CreditCardNumber	nvarchar(20) = null,
	@CreditCardExp		nvarchar(20) = null,
	@CreditCardSecCode	nvarchar(10) = null,
	@OrderGuid			uniqueidentifier = null output
as
begin
	select	@OrderGuid = newid();

	insert into data.Orders
	(
		OrderGuid,
		UserGuid,
		OrderName,
		OrderDate,
		SalesRep,
		CreditCardNumber,
		CreditCardExp,
		CreditCardSecCode
	)
	values
	(
		@OrderGuid,
		@UserGuid,
		@OrderName,
		@OrderDate,
		@SalesRep,
		@CreditCardNumber,
		@CreditCardExp,
		@CreditCardSecCode
	);
end
go

create proc [data].[DeleteOrder]
	@OrderGuid		uniqueidentifier = null
as
begin
	delete from data.Orders
	where	[OrderGuid] = @OrderGuid;
end
go

create proc [data].[UpdateOrder]
	@OrderGuid			uniqueidentifier = null,
	@OrderName			nvarchar(50) = null,
	@OrderDate			datetime2 = null,
	@SalesRep			sysname = null,
	@CreditCardNumber	nvarchar(20) = null,
	@CreditCardExp		nvarchar(20) = null,
	@CreditCardSecCode	nvarchar(10) = null
as
begin
	update
		[data].[Orders]
	set
		[OrderName] = @OrderName,
		[OrderDate] = @OrderDate,
		[SalesRep] = @SalesRep,
		[CreditCardNumber] = @CreditCardNumber,
		[CreditCardExp] = @CreditCardExp,
		[CreditCardSecCode] = @CreditCardSecCode,
		[DateUpdated] = getutcdate()
	where
		[OrderGuid] = @OrderGuid
	;
end
go

create proc [data].[GetOrders]
as
begin
	select
		u.UserGuid,
		u.UserName,
		u.FirstName,
		u.LastName,
		u.EMail,
		u.SSN,
		u.DoB,
		o.OrderGuid,
		o.OrderName,
		o.OrderDate,
		o.SalesRep,
		o.CreditCardNumber,
		o.CreditCardExp,
		o.CreditCardSecCode
	from
		data.Users u
		inner join data.Orders o on u.UserGuid = o.UserGuid
	;
end
go

create proc [data].[CreateOrderLine]
	@OrderGuid		uniqueidentifier = null,
	@ProductGuid	uniqueidentifier = null,
	@ProductQty		int = null,
	@UnitCost		float = null,
	@OrderLineGuid	uniqueidentifier = null output
as
begin
	select	@OrderLineGuid = newid();

	insert into data.OrderLines
	(
		OrderLineGuid,
		OrderGuid,
		ProductGuid,
		ProductQty,
		UnitCost
	)
	values
	(
		@OrderLineGuid,
		@OrderGuid,
		@ProductGuid,
		@ProductQty,
		@UnitCost
	);
end
go

create proc [data].[DeleteOrderLine]
	@OrderLineGuid		uniqueidentifier = null
as
begin
	delete from data.OrderLines
	where	[OrderLineGuid] = @OrderLineGuid;
end
go

create proc [data].[UpdateOrderLine]
	@OrderLineGuid		uniqueidentifier = null,
	@ProductQty			int = null,
	@UnitCost			float = null
as
begin
	update
		[data].[OrderLines]
	set
		[ProductQty] = @ProductQty,
		[UnitCost] = @UnitCost,
		[DateUpdated] = getutcdate()
	where
		[OrderLineGuid] = @OrderLineGuid
	;
end
go

create proc [data].[GetOrderLines]
as
begin
	select
		u.UserGuid,
		u.UserName,
		u.FirstName,
		u.LastName,
		u.EMail,
		u.SSN,
		u.DoB,
		o.OrderGuid,
		o.OrderName,
		o.OrderDate,
		o.CreditCardNumber,
		o.CreditCardExp,
		o.CreditCardSecCode,
		ol.OrderLineGuid,
		ol.OrderGuid,
		ol.ProductGuid,
		ol.ProductQty,
		ol.UnitCost
	from
		data.Users u
		inner join data.Orders o on u.UserGuid = o.UserGuid
		inner join data.OrderLines ol on o.OrderGuid = ol.OrderGuid
	;
end
go

-- END STORED PROCEDURES
-- ==================================================


-- ==================================================
-- BEGIN SAMPLE DATA
-- Creates users; orders for each users; and order lines for each order

truncate table data.Users;
truncate table data.Orders;
truncate table data.OrderLines;
go

set nocount on;
go

declare	@iUser			int,
		@vUser			varchar(10),
		@userMax		int,
		@iOrder			int,
		@vOrder			varchar(10),
		@orderMax		int,
		@iOrderLine		int,
		@orderLineMax	int,
		@UserGuid		uniqueidentifier,
		@OrderGuid		uniqueidentifier
;

declare	@UserName		nvarchar(50),
		@FirstName		nvarchar(50),
		@LastName		nvarchar(50),
		@EMail			nvarchar(50),
		@SSN			nvarchar(20),
		@DoB			datetime2(3)
;

declare	@OrderName			nvarchar(50),
		@OrderDate			datetime2,
		@SalesRep			sysname,
		@CreditCardNumber	nvarchar(20),
		@CreditCardExp		nvarchar(20),
		@CreditCardSecCode	nvarchar(10)
;

declare	@ProductGuid	uniqueidentifier,
		@ProductQty		int,
		@UnitCost		float
;

select	@iUser = 1,
		@userMax = 10,
		@iOrder = 1,
		@orderMax = 5,
		@iOrderLine = 1,
		@orderLineMax = 10
;

select	@DoB = '3/1/1990';

while (@iUser <= @userMax)
begin
	select	@vUser = convert(varchar(10), @iUser);

	select	@UserName = 'UserName' + @vUser,
			@FirstName = 'FirstName' + @vUser,
			@LastName = 'LastName' + @vUser,
			@Email = 'Email' + @vUser,
			@SSN = '111-22-' + convert(varchar(4), 3000 + @iUser),
			@DoB = dateadd(dd, @iUser, @DoB)
	;

	exec	data.CreateUser @UserName, @FirstName, @LastName, @Email, @SSN, @DoB, @UserGuid output;

	while (@iOrder <= @orderMax)
	begin
		select	@vOrder = convert(varchar(10), @iOrder);

		select	@OrderName = 'OrderName_' + @vUser + '_' + @vOrder,
				@OrderDate = dateadd(dd, @iOrder, getutcdate()),
				@SalesRep = 'SqlSalesPerson' + @vOrder,
				@CreditCardNumber = convert(nvarchar(20), 4000123412341111 + @iUser),
				@CreditCardExp = case when @iUser <= 12 then convert(nvarchar(10), @iUser) else convert(nvarchar(10), @iUser % 12) end + '/' + convert(nvarchar(10), year(getutcdate()) + @iUser),
				@CreditCardSecCode = convert(nvarchar(10), 1234 + @iUser)
		;

		exec	data.CreateOrder @UserGuid, @OrderName, @OrderDate, @SalesRep, @CreditCardNumber, @CreditCardExp, @CreditCardSecCode, @OrderGuid output;

		while (@iOrderLine < @orderLineMax)
		begin
			select	@ProductGuid = newid(),
					@ProductQty = 10 * @iOrderLine,
					@UnitCost = @iOrderLine + (2 * @iOrderLine / 10) + (5 * @iOrderLine / 100)
			;

			exec	data.CreateOrderLine @OrderGuid, @ProductGuid, @ProductQty, @UnitCost;

			select	@iOrderLine = @iOrderLine + 1;
		end

		select	@iOrderLine = 1;
		select	@iOrder = @iOrder + 1;
	end

	select	@iOrder = 1;
	select	@iUser = @iUser + 1;
end
go

-- END SAMPLE DATA
-- ==================================================
