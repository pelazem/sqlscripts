USE [master];
GO

CREATE DATABASE [OrdersDb]
	CONTAINMENT = NONE
	ON PRIMARY 
( NAME = N'OrdersDb', FILENAME = N'E:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\OrdersDb.mdf' , SIZE = 128MB , MAXSIZE = UNLIMITED, FILEGROWTH = 128MB )
 LOG ON 
( NAME = N'OrdersDb_log', FILENAME = N'E:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\OrdersDb_log.ldf' , SIZE = 64MB , MAXSIZE = 2048GB , FILEGROWTH = 64MB )
;
GO

ALTER DATABASE [OrdersDb] SET COMPATIBILITY_LEVEL = 130
GO

IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
	EXEC [OrdersDb].[dbo].[sp_fulltext_database] @action = 'enable';
end
GO

ALTER DATABASE [OrdersDb] SET RECOVERY SIMPLE;
GO
ALTER DATABASE [OrdersDb] SET  MULTI_USER;
GO
ALTER DATABASE [OrdersDb] SET READ_WRITE;
GO
ALTER DATABASE [OrdersDb] SET QUERY_STORE = ON;
GO


USE [OrdersDb];
go

-- ==================================================
-- BEGIN SCHEMAS
create schema [data];
go
create schema [etl];
go
-- END SCHEMAS
-- ==================================================


-- ==================================================
-- BEGIN ROLES
CREATE ROLE [GeneratorRole];
go
CREATE ROLE [ETLRole];
go
-- END ROLES
-- ==================================================


-- ==================================================
-- BEGIN Orders
CREATE USER [generator] FOR LOGIN [generator] WITH DEFAULT_SCHEMA=[data]
GO
-- END Orders
-- ==================================================


-- ==================================================
-- BEGIN SECURITY
grant execute, select on schema :: [data] to [GeneratorRole];
go
grant execute, select, insert, update, delete on schema :: [etl] to [ETLRole];
go
-- END SECURITY
-- ==================================================


-- ==================================================
-- BEGIN TABLES

create table [data].[Orders]
(
	[OrderGuid] [uniqueidentifier] not null,
	[UserGuid] [uniqueidentifier] not null,
	[OrderName] [nvarchar](50) null,
	[OrderDate] [datetime] null,
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
alter table [data].[Orders] add constraint [PK_data_Orders_OrderGuid] primary key nonclustered (OrderGuid);
go

create table [data].[OrderLines]
(
	[OrderLineGuid] [uniqueidentifier] not null,
	[OrderGuid] [uniqueidentifier] not null,
	[ProductGuid] [uniqueidentifier] not null,
	[ProductQty] [int] null,
	[DateCreated] [datetime2] null,
	[DateUpdated] [datetime2] null
)
on [primary];
go

alter table [data].[OrderLines] add constraint [DF_data_OrderLines_OrderLineGuid] default (newsequentialid()) for [OrderLineGuid];
alter table [data].[OrderLines] add constraint [DF_data_OrderLines_DateCreated] default (getutcdate()) for [DateCreated];
alter table [data].[OrderLines] add constraint [DF_data_OrderLines_DateUpdated] default (getutcdate()) for [DateUpdated];
go
alter table [data].[OrderLines] add constraint [PK_data_OrderLines_OrderGuid] primary key nonclustered (OrderLineGuid);
go




create table [etl].[Queue]
(
	[ChangeGuid] [uniqueidentifier] not null,
	[BatchGuid] [uniqueidentifier] null,
	[EventType] [nvarchar](20) null,
	[EventDateTime] [datetime2] not null,
	[DataItemSource] [nvarchar](50) null,
	[DataItemGuid] [uniqueidentifier] null,
	[EventData] [nvarchar](max) null
)
on [primary];
go

alter table [etl].[Queue] add constraint [DF_etl_Queue_ChangeGuid] default (newsequentialid()) for [ChangeGuid];
alter table [etl].[Queue] add constraint [DF_etl_Queue_EventDateTime] default (getutcdate()) for [EventDateTime];
go
alter table [etl].[Queue] add constraint [PK_etl_Queue_ChangeGuid] primary key nonclustered (ChangeGuid);
go

CREATE NONCLUSTERED INDEX [ixEventDateTime] ON [etl].[Queue]
(
	[EventDateTime] ASC)
INCLUDE
(
	[ChangeGuid]
)
WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF);
go

CREATE CLUSTERED INDEX [ixcBatchGuid] ON [etl].[Queue]
(
	[BatchGuid] ASC
)
WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF);
go


-- END TABLES
-- ==================================================


-- ==================================================
-- BEGIN TRIGGERS

create trigger [data].[QueueDelete_data_Orders]
on [data].[Orders]
after delete
as
begin
	set nocount on;

	insert into
		[etl].[Queue]
		(
			[EventType],
			[DataItemSource],
			[DataItemGuid],
			[EventData]
		)
	select
		'DELETE',
		'data.Orders',
		[OrderGuid],
		JSON_QUERY((select OrderGuid, UserGuid, OrderName, OrderDate, DateCreated, DateUpdated for json path, without_array_wrapper))
	from
		deleted;
end
go

create trigger [data].[QueueCreate_data_Orders]
on [data].[Orders]
after insert
as
begin
	set nocount on;

	insert into
		[etl].[Queue]
		(
			[EventType],
			[DataItemSource],
			[DataItemGuid],
			[EventData]
		)
	select
		'CREATE',
		'data.Orders',
		[OrderGuid],
		JSON_QUERY((select OrderGuid, UserGuid, OrderName, OrderDate, DateCreated, DateUpdated for json path, without_array_wrapper))
	from
		inserted;
end
go

create trigger [data].[QueueUpdate_data_Orders]
on [data].[Orders]
after update
as
begin
	set nocount on;

	insert into
		[etl].[Queue]
		(
			[EventType],
			[DataItemSource],
			[DataItemGuid],
			[EventData]
		)
	select
		'UPDATE',
		'data.Orders',
		[OrderGuid],
		JSON_QUERY((select OrderGuid, UserGuid, OrderName, OrderDate, DateCreated, DateUpdated for json path, without_array_wrapper))
	from
		inserted;
end
go

create trigger [data].[QueueDelete_data_OrderLines]
on [data].[OrderLines]
after delete
as
begin
	set nocount on;

	insert into
		[etl].[Queue]
		(
			[EventType],
			[DataItemSource],
			[DataItemGuid],
			[EventData]
		)
	select
		'DELETE',
		'data.OrderLines',
		[OrderLineGuid],
		JSON_QUERY((select OrderLineGuid, OrderGuid, ProductGuid, ProductQty, DateCreated, DateUpdated for json path, without_array_wrapper))
	from
		deleted;
end
go

create trigger [data].[QueueCreate_data_OrderLines]
on [data].[OrderLines]
after insert
as
begin
	set nocount on;

	insert into
		[etl].[Queue]
		(
			[EventType],
			[DataItemSource],
			[DataItemGuid],
			[EventData]
		)
	select
		'CREATE',
		'data.OrderLines',
		[OrderLineGuid],
		JSON_QUERY((select OrderLineGuid, OrderGuid, ProductGuid, ProductQty, DateCreated, DateUpdated for json path, without_array_wrapper))
	from
		inserted;
end
go

create trigger [data].[QueueUpdate_data_OrderLines]
on [data].[OrderLines]
after update
as
begin
	set nocount on;

	insert into
		[etl].[Queue]
		(
			[EventType],
			[DataItemSource],
			[DataItemGuid],
			[EventData]
		)
	select
		'UPDATE',
		'data.OrderLines',
		[OrderLineGuid],
		JSON_QUERY((select OrderLineGuid, OrderGuid, ProductGuid, ProductQty, DateCreated, DateUpdated for json path, without_array_wrapper))
	from
		inserted;
end
go

-- END TRIGGERS
-- ==================================================


-- ==================================================
-- BEGIN STORED PROCEDURES

create proc [data].[CreateOrder]
	@UserGuid		uniqueidentifier = null,
	@OrderName		nvarchar(50) = null,
	@OrderGuid		uniqueidentifier = null output
as
begin
	select	@OrderGuid = newid();

	--select @UserGuid = (select top 1 UserGuid from UsersDb.data.Users order by RAND(CHECKSUM(*) * RAND()));
	select @UserGuid = newid();

	insert into data.Orders
	(
		UserGuid,
		OrderName
	)
	values
	(
		@UserGuid,
		@OrderName
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
	@OrderGuid		uniqueidentifier = null,
	@OrderName		nvarchar(50) = null
as
begin
	update
		[data].[Orders]
	set
		[OrderName] = @OrderName,
		[DateUpdated] = getutcdate()
	where
		[OrderGuid] = @OrderGuid
	;
end
go

create proc [data].[CreateOrderLine]
	@OrderGuid		uniqueidentifier = null,
	@ProductGuid	uniqueidentifier = null,
	@ProductQty		int = 1,
	@OrderLineGuid	uniqueidentifier = null output
as
begin
	select	@OrderLineGuid = newid();

	--select @OrderGuid = (select top 1 OrderGuid from data.Orders order by RAND(CHECKSUM(*) * RAND()));
	--select @ProductGuid = (select top 1 ProductGuid from ProductsDb.data.Products order by RAND(CHECKSUM(*) * RAND()));
	select @OrderGuid = newid();
	select @ProductGuid = newid();

	insert into data.OrderLines
	(
		OrderGuid,
		ProductGuid,
		ProductQty
	)
	values
	(
		@OrderGuid,
		@ProductGuid,
		@ProductQty
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
	@ProductQty			int = 2
as
begin
	update
		[data].[OrderLines]
	set
		[ProductQty] = @ProductQty,
		[DateUpdated] = getutcdate()
	where
		[OrderLineGuid] = @OrderLineGuid
	;
end
go

create proc [etl].[GetChanges]
	@batchSize	int = 1000
as
begin
	declare	@BatchGuid	uniqueidentifier;
	select	@BatchGuid = newid();

	-- PREP BATCH
	update
		[etl].[Queue]
	set
		[BatchGuid] = @BatchGuid
	where
		[ChangeGuid] in
		(
			select
				[ChangeGuid]
			from
				[etl].[Queue]
			where
				[BatchGuid] is null
			order by
				[EventDateTime]
			OFFSET 0 ROWS
			FETCH NEXT @batchSize ROWS only
		)
	;

	-- RETRIEVE
	select
		[BatchGuid],
		[EventType],
		[EventDateTime],
		[DataItemSource],
		[DataItemGuid],
		[EventData]
	from
		[etl].[Queue]
	where
		[BatchGuid] = @BatchGuid
	;	

	-- CLEANUP
	delete from
		[etl].[Queue]
	where
		[BatchGuid] = @BatchGuid
end
go

-- END STORED PROCEDURES
-- ==================================================
