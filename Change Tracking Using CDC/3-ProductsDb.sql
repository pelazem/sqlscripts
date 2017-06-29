USE [master];
GO

CREATE DATABASE [ProductsDb]
	CONTAINMENT = NONE
	ON PRIMARY 
( NAME = N'ProductsDb', FILENAME = N'D:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\ProductsDb.mdf' , SIZE = 128MB , MAXSIZE = UNLIMITED, FILEGROWTH = 128MB )
 LOG ON 
( NAME = N'ProductsDb_log', FILENAME = N'D:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\ProductsDb_log.ldf' , SIZE = 64MB , MAXSIZE = 2048GB , FILEGROWTH = 64MB )
;
GO

ALTER DATABASE [ProductsDb] SET COMPATIBILITY_LEVEL = 130
GO

IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
	EXEC [ProductsDb].[dbo].[sp_fulltext_database] @action = 'enable';
end
GO

ALTER DATABASE [ProductsDb] SET RECOVERY SIMPLE;
GO
ALTER DATABASE [ProductsDb] SET  MULTI_USER;
GO
ALTER DATABASE [ProductsDb] SET READ_WRITE;
GO
ALTER DATABASE [ProductsDb] SET QUERY_STORE = ON;
GO


USE [ProductsDb];
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

create table [data].[ProductCategories]
(
	[ProductCategoryId] [int] identity(1,1) constraint [PK_data_ProductCategories_ProductCategoryId] primary key not null,
	[ProductCategoryGuid] [uniqueidentifier] not null,
	[ProductCategoryName] [nvarchar](50) null,
	[DateCreated] [datetime2] null,
	[DateUpdated] [datetime2] null,
	[ValidFrom] [datetime2](2) GENERATED ALWAYS AS ROW START, 
	[ValidTo] [datetime2](2) GENERATED ALWAYS AS ROW END,
	PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)  
)    
ON [PRIMARY]
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [data].[ProductCategoriesHistory]))
go
alter table [data].[ProductCategories] add constraint [DF_data_ProductCategories_ProductCategoryGuid] default (newsequentialid()) for [ProductCategoryGuid];
alter table [data].[ProductCategories] add constraint [DF_data_ProductCategories_DateCreated] default (getutcdate()) for [DateCreated];
alter table [data].[ProductCategories] add constraint [DF_data_ProductCategories_DateUpdated] default (getutcdate()) for [DateUpdated];
go

-- ENABLE CDC for table
exec sys.sp_cdc_enable_table
	@source_schema = 'data',
	@source_name = 'ProductCategories',
	@role_name = 'ETLRole',
	@supports_net_changes = 1,
	@index_name = 'PK_data_ProductCategories_ProductCategoryId',
	@captured_column_list = null
;
go

create table [data].[Products]
(
	[ProductId] [int] identity(1,1) constraint [PK_data_Products_ProductId] primary key not null,
	[ProductGuid] [uniqueidentifier] not null,
	[ProductName] [nvarchar](50) null,
	[DateCreated] [datetime2] null,
	[DateUpdated] [datetime2] null,
	[ValidFrom] [datetime2](2) GENERATED ALWAYS AS ROW START, 
	[ValidTo] [datetime2](2) GENERATED ALWAYS AS ROW END,
	PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)  
)    
ON [PRIMARY]
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [data].[ProductsHistory]))
go
alter table [data].[Products] add constraint [DF_data_Products_ProductGuid] default (newsequentialid()) for [ProductGuid];
alter table [data].[Products] add constraint [DF_data_Products_DateCreated] default (getutcdate()) for [DateCreated];
alter table [data].[Products] add constraint [DF_data_Products_DateUpdated] default (getutcdate()) for [DateUpdated];
go

-- ENABLE CDC for table
exec sys.sp_cdc_enable_table
	@source_schema = 'data',
	@source_name = 'Products',
	@role_name = 'ETLRole',
	@supports_net_changes = 1,
	@index_name = 'PK_data_Products_ProductId',
	@captured_column_list = null
;
go

create table [data].[ProductCategoriesProducts]
(
	[MappingId] [int] identity(1,1) constraint [PK_data_ProductCategoriesProducts_MappingId] primary key not null,
	[MappingGuid] [uniqueidentifier] not null,
	[ProductCategoryGuid] [uniqueidentifier] not null,
	[ProductGuid] [uniqueidentifier] not null,
	[DateCreated] [datetime2] null,
	[DateUpdated] [datetime2] null,
	[ValidFrom] [datetime2](2) GENERATED ALWAYS AS ROW START, 
	[ValidTo] [datetime2](2) GENERATED ALWAYS AS ROW END,
	PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)  
)    
ON [PRIMARY]
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [data].[ProductCategoriesProductsHistory]))
go
alter table [data].[ProductCategoriesProducts] add constraint [DF_data_ProductCategoriesProducts_MappingGuid] default (newsequentialid()) for [MappingGuid];
alter table [data].[ProductCategoriesProducts] add constraint [DF_data_ProductCategoriesProducts_DateCreated] default (getutcdate()) for [DateCreated];
alter table [data].[ProductCategoriesProducts] add constraint [DF_data_ProductCategoriesProducts_DateUpdated] default (getutcdate()) for [DateUpdated];
go

-- ENABLE CDC for table
exec sys.sp_cdc_enable_table
	@source_schema = 'data',
	@source_name = 'ProductCategoriesProducts',
	@role_name = 'ETLRole',
	@supports_net_changes = 1,
	@index_name = 'PK_data_ProductCategoriesProducts_MappingId',
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

create proc [data].[GetProductCategories]
	@IncludeHistory	bit = 0
as
begin
	if	@IncludeHistory = 1
		begin
			SELECT
				pc.[ProductCategoryId],
				pc.[ProductCategoryGuid],
				pc.[ProductCategoryName],
				pc.[ValidFrom],
				pc.[ValidTo],
				IIF (YEAR(pc.[ValidTo]) = 9999, 1, 0) AS [IsActual]
			FROM
				[data].[ProductCategories]
			FOR SYSTEM_TIME ALL AS pc
			ORDER BY
				pc.[ProductCategoryName],
				pc.[ValidTo] desc
			;
		end
	else
		begin
			SELECT
				pc.[ProductCategoryId],
				pc.[ProductCategoryGuid],
				pc.[ProductCategoryName],
				pc.[ValidFrom],
				pc.[ValidTo],
				[IsActual] = 1
			FROM
				[data].[ProductCategories] pc
			ORDER BY
				pc.[ProductCategoryName],
				pc.[ValidTo] desc
			;
		end
end
go

create proc [data].[GetProducts]
	@IncludeHistory	bit = 0
as
begin
	if	@IncludeHistory = 1
		begin
			SELECT
				pc.[ProductId],
				pc.[ProductGuid],
				pc.[ProductName],
				pc.[ValidFrom],
				pc.[ValidTo],
				IIF (YEAR(pc.[ValidTo]) = 9999, 1, 0) AS [IsActual]
			FROM
				[data].[Products]
			FOR SYSTEM_TIME ALL AS pc
			ORDER BY
				pc.[ProductName],
				pc.[ValidTo] desc
			;
		end
	else
		begin
			SELECT
				pc.[ProductId],
				pc.[ProductGuid],
				pc.[ProductName],
				pc.[ValidFrom],
				pc.[ValidTo],
				[IsActual] = 1
			FROM
				[data].[Products] pc
			ORDER BY
				pc.[ProductName],
				pc.[ValidTo] desc
			;
		end
end
go

create proc [data].[GetProductsByProductCategory]
	@ProductCategoryGuid	uniqueidentifier = null,
	@IncludeHistory			bit = 0
as
begin
	if	@IncludeHistory = 1
		begin
			SELECT
				p.[ProductId],
				p.[ProductGuid],
				p.[ProductName],
				p.[ValidFrom],
				p.[ValidTo],
				IIF (YEAR(p.[ValidTo]) = 9999, 1, 0) AS [IsActual]
			FROM
				[data].[Products] FOR SYSTEM_TIME ALL p
				inner join [data].[ProductCategoriesProducts] pcp on pcp.[ProductGuid] = p.[ProductGuid]
			WHERE
				pcp.[ProductCategoryGuid] = @ProductCategoryGuid
			ORDER BY
				p.[ProductName],
				p.[ValidTo] desc
			;
		end
	else
		begin
			SELECT
				p.[ProductId],
				p.[ProductGuid],
				p.[ProductName],
				p.[ValidFrom],
				p.[ValidTo],
				[IsActual] = 1
			FROM
				[data].[Products] p
				inner join [data].[ProductCategoriesProducts] pcp on pcp.[ProductGuid] = p.[ProductGuid]
			WHERE
				pcp.[ProductCategoryGuid] = @ProductCategoryGuid
			ORDER BY
				p.[ProductName],
				p.[ValidTo] desc
			;
		end
end
go


create proc [data].[CreateProductCategory]
	@ProductCategoryName		nvarchar(50),
	@ProductCategoryGuid		uniqueidentifier = null output
as
begin
	select	@ProductCategoryGuid = newid();

	insert into data.ProductCategories
	(
		ProductCategoryGuid,
		ProductCategoryName
	)
	values
	(
		@ProductCategoryGuid,
		@ProductCategoryName
	);
end
go

create proc [data].[DeleteProductCategory]
	@ProductCategoryGuid		uniqueidentifier
as
begin
	delete from data.ProductCategories
	where	[ProductCategoryGuid] = @ProductCategoryGuid;
end
go

create proc [data].[UpdateProductCategory]
	@ProductCategoryGuid		uniqueidentifier,
	@ProductCategoryName		nvarchar(50)
as
begin
	update
		[data].[ProductCategories]
	set
		[ProductCategoryName] = @ProductCategoryName
	where
		[ProductCategoryGuid] = @ProductCategoryGuid
	;
end
go


create proc [data].[CreateProduct]
	@ProductName		nvarchar(50),
	@ProductGuid		uniqueidentifier = null output
as
begin
	select	@ProductGuid = newid();

	insert into data.Products
	(
		ProductGuid,
		ProductName
	)
	values
	(
		@ProductGuid,
		@ProductName
	);
end
go

create proc [data].[DeleteProduct]
	@ProductGuid		uniqueidentifier
as
begin
	delete from data.Products
	where	[ProductGuid] = @ProductGuid;
end
go

create proc [data].[UpdateProduct]
	@ProductGuid		uniqueidentifier,
	@ProductName			nvarchar(50)
as
begin
	update
		[data].[Products]
	set
		[ProductName] = @ProductName
	where
		[ProductGuid] = @ProductGuid
	;
end
go


create proc [data].[CreateProductCategoryProduct]
	@ProductCategoryGuid		uniqueidentifier,
	@ProductGuid				uniqueidentifier,
	@MappingGuid				uniqueidentifier = null output
as
begin
	select	@MappingGuid = newid();

	insert into data.ProductCategoriesProducts
	(
		MappingGuid,
		ProductCategoryGuid,
		ProductGuid
	)
	values
	(
		@MappingGuid,
		@ProductCategoryGuid,
		@ProductGuid
	);
end
go

create proc [data].[DeleteProductCategoryProduct]
	@MappingId		int
as
begin
	delete from data.ProductCategoriesProducts
	where	[MappingId] = @MappingId;
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

create proc [etl].[GetChangesProductCategories]
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

	set		@captureInstance = 'data_ProductCategories';
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
			[EventData] = JSON_QUERY((select ProductCategoryId, ProductCategoryGuid, ProductCategoryName, DateCreated, DateUpdated for json path, without_array_wrapper))
		from
			cdc.fn_cdc_get_all_changes_data_ProductCategories(@from_lsn, @to_lsn, @scope) cdc
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

create proc [etl].[GetChangesProducts]
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

	set		@captureInstance = 'data_Products';
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
			[EventData] = JSON_QUERY((select ProductId, ProductGuid, ProductName, DateCreated, DateUpdated for json path, without_array_wrapper))
		from
			cdc.fn_cdc_get_all_changes_data_Products(@from_lsn, @to_lsn, @scope) cdc
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

create proc [etl].[GetChangesProductCategoriesProducts]
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

	set		@captureInstance = 'data_ProductCategoriesProducts';
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
			[EventData] = JSON_QUERY((select MappingId, MappingGuid, ProductCategoryGuid, ProductGuid, DateCreated, DateUpdated for json path, without_array_wrapper))
		from
			cdc.fn_cdc_get_all_changes_data_ProductCategoriesProducts(@from_lsn, @to_lsn, @scope) cdc
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
