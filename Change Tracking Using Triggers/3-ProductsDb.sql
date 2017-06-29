USE [master];
GO

CREATE DATABASE [ProductsDb]
	CONTAINMENT = NONE
	ON PRIMARY 
( NAME = N'ProductsDb', FILENAME = N'E:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\ProductsDb.mdf' , SIZE = 128MB , MAXSIZE = UNLIMITED, FILEGROWTH = 128MB )
 LOG ON 
( NAME = N'ProductsDb_log', FILENAME = N'E:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\ProductsDb_log.ldf' , SIZE = 64MB , MAXSIZE = 2048GB , FILEGROWTH = 64MB )
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
	[ProductCategoryId] [int] identity(1,1) primary key not null,
	[ProductCategoryGuid] [uniqueidentifier] not null,
	[ProductCategoryName] [nvarchar](50) null,
	[DateCreated] [datetime2] null,
	[DateUpdated] [datetime2] null,
	[ValidFrom] datetime2 (2) GENERATED ALWAYS AS ROW START, 
	[ValidTo] datetime2 (2) GENERATED ALWAYS AS ROW END,
	PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)  
)    
ON [PRIMARY]
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [data].[ProductCategoriesHistory]))
go
alter table [data].[ProductCategories] add constraint [DF_data_ProductCategories_ProductCategoryGuid] default (newsequentialid()) for [ProductCategoryGuid];
alter table [data].[ProductCategories] add constraint [DF_data_ProductCategories_DateCreated] default (getutcdate()) for [DateCreated];
alter table [data].[ProductCategories] add constraint [DF_data_ProductCategories_DateUpdated] default (getutcdate()) for [DateUpdated];
go

create table [data].[Products]
(
	[ProductId] [int] identity(1,1) primary key not null,
	[ProductGuid] [uniqueidentifier] not null,
	[ProductName] [nvarchar](50) null,
	[DateCreated] [datetime2] null,
	[DateUpdated] [datetime2] null,
	[ValidFrom] datetime2 (2) GENERATED ALWAYS AS ROW START, 
	[ValidTo] datetime2 (2) GENERATED ALWAYS AS ROW END,
	PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)  
)    
ON [PRIMARY]
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [data].[ProductsHistory]))
go
alter table [data].[Products] add constraint [DF_data_Products_ProductGuid] default (newsequentialid()) for [ProductGuid];
alter table [data].[Products] add constraint [DF_data_Products_DateCreated] default (getutcdate()) for [DateCreated];
alter table [data].[Products] add constraint [DF_data_Products_DateUpdated] default (getutcdate()) for [DateUpdated];
go

create table [data].[ProductCategoriesProducts]
(
	[ProductCategoryId] [int] primary key not null,
	[ProductCategoryGuid] [uniqueidentifier] not null,
	[ProductId] [int] not null,
	[ProductGuid] [uniqueidentifier] not null,
	[DateCreated] [datetime2] null,
	[DateUpdated] [datetime2] null,
	[ValidFrom] datetime2 (2) GENERATED ALWAYS AS ROW START, 
	[ValidTo] datetime2 (2) GENERATED ALWAYS AS ROW END,
	PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)  
)    
ON [PRIMARY]
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [data].[ProductCategoriesProductsHistory]))
go
alter table [data].[ProductCategoriesProducts] add constraint [DF_data_ProductCategoriesProducts_DateCreated] default (getutcdate()) for [DateCreated];
alter table [data].[ProductCategoriesProducts] add constraint [DF_data_ProductCategoriesProducts_DateUpdated] default (getutcdate()) for [DateUpdated];
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

create trigger [data].[QueueDelete_data_Products]
on [data].[Products]
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
		'data.Products',
		[ProductGuid],
		JSON_QUERY((select ProductId, ProductGuid, ProductName, DateCreated, DateUpdated for json path, without_array_wrapper))
	from
		deleted;
end
go

create trigger [data].[QueueCreate_data_Products]
on [data].[Products]
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
		'data.Products',
		[ProductGuid],
		JSON_QUERY((select ProductId, ProductGuid, ProductName, DateCreated, DateUpdated for json path, without_array_wrapper))
	from
		inserted;
end
go

create trigger [data].[QueueUpdate_data_Products]
on [data].[Products]
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
		'data.Products',
		[ProductGuid],
		JSON_QUERY((select ProductId, ProductGuid, ProductName, DateCreated, DateUpdated for json path, without_array_wrapper))
	from
		inserted;
end
go

-- END TRIGGERS
-- ==================================================

-- ==================================================
-- BEGIN STORED PROCEDURES

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
				pc.[DateCreated],
				pc.[DateUpdated],
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
				pc.[DateCreated],
				pc.[DateUpdated],
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
				pc.[DateCreated],
				pc.[DateUpdated],
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
				pc.[DateCreated],
				pc.[DateUpdated],
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
	@ProductCategoryId		int = null,
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
				[data].[Products].[ValidTo],
				IIF (YEAR(p.[ValidTo]) = 9999, 1, 0) AS [IsActual]
			FROM
				[data].[Products] FOR SYSTEM_TIME ALL p
				inner join [data].[ProductCategoriesProducts] pcp on pcp.[ProductId] = p.[ProductId]
			WHERE
				(coalesce(@ProductCategoryId, 0) > 0 and pcp.[ProductCategoryId] = @ProductCategoryId)
				or
				(coalesce(@ProductCategoryId, 0) <= 0 and pcp.[ProductCategoryGuid] = @ProductCategoryGuid)
			ORDER BY
				p.[ProductName],
				p.[ValidTo] desc
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
				inner join [data].[ProductCategoriesProducts] pcp on pcp.[ProductId] = pc.[ProductId]
			WHERE
				(coalesce(@ProductCategoryId, 0) > 0 and pcp.[ProductCategoryId] = @ProductCategoryId)
				or
				(coalesce(@ProductCategoryId, 0) <= 0 and pcp.[ProductCategoryGuid] = @ProductCategoryGuid)
			ORDER BY
				pc.[ProductName],
				pc.[ValidTo] desc
			;
		end
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


-- ==================================================
-- BEGIN SAMPLE DATA

insert into [data].[ProductCategories] (ProductCategoryName) values ('Household');
insert into [data].[ProductCategories] (ProductCategoryName) values ('Learning');
insert into [data].[ProductCategories] (ProductCategoryName) values ('Food');
insert into [data].[ProductCategories] (ProductCategoryName) values ('Automotive');
insert into [data].[ProductCategories] (ProductCategoryName) values ('Music');
insert into [data].[ProductCategories] (ProductCategoryName) values ('Movies');
insert into [data].[ProductCategories] (ProductCategoryName) values ('Parenting');
insert into [data].[ProductCategories] (ProductCategoryName) values ('Books');
insert into [data].[ProductCategories] (ProductCategoryName) values ('Cleaning');
insert into [data].[ProductCategories] (ProductCategoryName) values ('Clothing');


-- END SAMPLE DATA
-- ==================================================
