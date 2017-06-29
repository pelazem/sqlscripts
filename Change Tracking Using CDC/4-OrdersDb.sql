USE [master];
GO

CREATE DATABASE [OrdersDb]
	CONTAINMENT = NONE
	ON PRIMARY 
( NAME = N'OrdersDb', FILENAME = N'D:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\OrdersDb.mdf' , SIZE = 128MB , MAXSIZE = UNLIMITED, FILEGROWTH = 128MB )
 LOG ON 
( NAME = N'OrdersDb_log', FILENAME = N'D:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\OrdersDb_log.ldf' , SIZE = 64MB , MAXSIZE = 2048GB , FILEGROWTH = 64MB )
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
-- ENABLE CDC for database
EXECUTE sys.sp_cdc_enable_db;  
GO
-- ==================================================


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
-- BEGIN USERS
CREATE USER [generator] FOR LOGIN [generator] WITH DEFAULT_SCHEMA=[data]
GO

ALTER ROLE [GeneratorRole] ADD MEMBER [generator];
ALTER ROLE [ETLRole] ADD MEMBER [generator];
-- END USERS
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
	[OrderGuid] [uniqueidentifier] constraint [PK_data_Orders_OrderGuid] primary key nonclustered not null,
	[UserGuid] [uniqueidentifier] null,
	[OrderName] [nvarchar](50) null,
	[OrderDate] [datetime2] null,
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

-- ENABLE CDC for table
exec sys.sp_cdc_enable_table
	@source_schema = 'data',
	@source_name = 'Orders',
	@role_name = 'ETLRole',
	@supports_net_changes = 1,
	@index_name = 'PK_data_Orders_OrderGuid',
	@captured_column_list = null
;
go

create table [data].[OrderLines]
(
	[OrderLineGuid] [uniqueidentifier] constraint [PK_data_OrderLines_OrderLineGuid] primary key nonclustered not null,
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

-- ENABLE CDC for table
exec sys.sp_cdc_enable_table
	@source_schema = 'data',
	@source_name = 'OrderLines',
	@role_name = 'ETLRole',
	@supports_net_changes = 1,
	@index_name = 'PK_data_OrderLines_OrderLineGuid',
	@captured_column_list = null
;
go


create table [etl].[Changes]
(
	[CaptureInstance]	[nvarchar](300) not null,
	[Lsn] [binary](10) not null
)
on [primary];
go

CREATE CLUSTERED INDEX [ixcCaptureInstance] ON [etl].[Changes]
(
	[CaptureInstance] ASC
)
WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF);
go

-- END TABLES
-- ==================================================


-- ==================================================
-- BEGIN STORED PROCEDURES

-- BEGIN ENTITY-SPECIFIC DML

create proc [data].[CreateOrder]
	@UserGuid		uniqueidentifier = null,
	@OrderName		nvarchar(50) = null,
	@OrderGuid		uniqueidentifier = null output
as
begin
	select	@OrderGuid = newid();

	insert into data.Orders
	(
		OrderGuid,
		UserGuid,
		OrderName
	)
	values
	(
		@OrderGuid,
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

	insert into data.OrderLines
	(
		OrderLineGuid,
		OrderGuid,
		ProductGuid,
		ProductQty
	)
	values
	(
		@OrderLineGuid,
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

-- END ENTITY-SPECIFIC DML

-- BEGIN GENERIC CHANGE RETRIEVAL

create proc [etl].[SaveTableCdcLsnUpper]
	@captureInstance	nvarchar(300),
	@lsn		binary(10)
as
begin
	set	@Lsn = @Lsn;

	if exists (select CaptureInstance from etl.Changes where CaptureInstance = @captureInstance)
		begin
			update	etl.Changes
			set		Lsn = @Lsn
			where	CaptureInstance = @captureInstance;
		end
	else
		begin
			insert into	etl.Changes (CaptureInstance, Lsn)
			values	(@captureInstance, @Lsn);
		end
end
go

create proc [etl].[GetTableCdcLsnLower]
	@captureInstance	nvarchar(300),
	@lsn				binary(10) output
as
begin
	declare	@lsnStored	binary(10),
			@lsnCdc		binary(10);

	-- Get latest one stored
	select	@lsnStored = Lsn
	from	etl.Changes
	where	CaptureInstance = @captureInstance;

	-- Get latest one tracked
	select	@lsnCdc = sys.fn_cdc_get_min_lsn(@captureInstance);

	-- Use stored unless tracked is higher - cannot call get changes function with LSNs outside available range
	select @lsn = case
		when @lsnStored is null or @lsnStored < @lsnCdc then @lsnCdc
		else @lsnStored
	end;
end
go

-- END GENERIC CHANGE RETRIEVAL

-- BEGIN ENTITY-SPECIFIC CHANGE RETRIEVAL

create proc [etl].[GetChangesOrders]
as
begin
	-- Generic
	declare	@batchGuid			uniqueidentifier,
			@scope				nvarchar(30),
			@eventTypeDelete	varchar(20),
			@eventTypeCreate	varchar(20),
			@eventTypeUpdate	varchar(20),
			@eventTypeNA		varchar(20),
			@from_lsn			binary(10),
			@to_lsn				binary(10);

	set		@batchGuid = newid();
	set		@scope = 'all';
	set		@eventTypeDelete = 'DELETE';
	set		@eventTypeCreate = 'CREATE';
	set		@eventTypeUpdate = 'UPDATE';
	set		@eventTypeNA = 'N/A';
	-- ----------

	-- Entity-specific
	declare	@captureInstance	nvarchar(300);

	set		@captureInstance = 'data_Orders';
	-- ----------

	begin transaction;

	begin try
		-- Get last saved lower LSN for CDC changes for this table
		exec	etl.GetTableCdcLsnLower @captureInstance, @from_lsn output;

		-- Get upper LSN
		select	@to_lsn = sys.fn_cdc_get_max_lsn();

		select
			[BatchGuid] = @BatchGuid,
			[EventType] = case
				when cdc.__$operation = 1 then @eventTypeDelete
				when cdc.__$operation = 2 then @eventTypeCreate
				when cdc.__$operation in (3, 4) then @eventTypeUpdate
				else @eventTypeNA
			end,
			[EventDateTime] = cast(sys.fn_cdc_map_lsn_to_time(cdc.__$start_lsn) as datetime2),
			[DataItemSource] = @captureInstance,
			[EventData] = JSON_QUERY((select OrderGuid, UserGuid, OrderName, OrderDate, DateCreated, DateUpdated for json path, without_array_wrapper))
		from
			cdc.fn_cdc_get_all_changes_data_Orders(@from_lsn, @to_lsn, @scope) cdc
		where
			cdc.__$start_lsn > @from_lsn
		;

		 -- Update upper LSN used for next batch retrieval
		 exec	etl.SaveTableCdcLsnUpper @captureInstance, @to_lsn;
	end try
	begin catch
		if	@@trancount > 0
			rollback transaction;
	end catch

	if	@@trancount > 0
		commit transaction;
end
go

create proc [etl].[GetChangesOrderLines]
as
begin
	-- Generic
	declare	@batchGuid			uniqueidentifier,
			@scope				nvarchar(30),
			@eventTypeDelete	varchar(20),
			@eventTypeCreate	varchar(20),
			@eventTypeUpdate	varchar(20),
			@eventTypeNA		varchar(20),
			@from_lsn			binary(10),
			@to_lsn				binary(10);

	set		@batchGuid = newid();
	set		@scope = 'all';
	set		@eventTypeDelete = 'DELETE';
	set		@eventTypeCreate = 'CREATE';
	set		@eventTypeUpdate = 'UPDATE';
	set		@eventTypeNA = 'N/A';
	-- ----------

	-- Entity-specific
	declare	@captureInstance	nvarchar(300);

	set		@captureInstance = 'data_OrderLines';
	-- ----------

	begin transaction;

	begin try
		-- Get last saved lower LSN for CDC changes for this table
		exec	etl.GetTableCdcLsnLower @captureInstance, @from_lsn output;

		-- Get upper LSN
		select	@to_lsn = sys.fn_cdc_get_max_lsn();

		select
			[BatchGuid] = @BatchGuid,
			[EventType] = case
				when cdc.__$operation = 1 then @eventTypeDelete
				when cdc.__$operation = 2 then @eventTypeCreate
				when cdc.__$operation in (3, 4) then @eventTypeUpdate
				else @eventTypeNA
			end,
			[EventDateTime] = cast(sys.fn_cdc_map_lsn_to_time(cdc.__$start_lsn) as datetime2),
			[DataItemSource] = @captureInstance,
			[EventData] = JSON_QUERY((select OrderLineGuid, OrderGuid, ProductGuid, ProductQty, DateCreated, DateUpdated for json path, without_array_wrapper))
		from
			cdc.fn_cdc_get_all_changes_data_OrderLines(@from_lsn, @to_lsn, @scope) cdc
		where
			cdc.__$start_lsn > @from_lsn
		;

		 -- Update upper LSN used for next batch retrieval
		 exec	etl.SaveTableCdcLsnUpper @captureInstance, @to_lsn;
	end try
	begin catch
		if	@@trancount > 0
			rollback transaction;
	end catch

	if	@@trancount > 0
		commit transaction;
end
go

-- END ENTITY-SPECIFIC CHANGE RETRIEVAL

-- END STORED PROCEDURES
-- ==================================================
