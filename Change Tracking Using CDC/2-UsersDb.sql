USE [master];
GO

CREATE DATABASE [UsersDb]
	CONTAINMENT = NONE
	ON PRIMARY 
( NAME = N'UsersDb', FILENAME = N'D:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\UsersDb.mdf', SIZE = 128MB , MAXSIZE = UNLIMITED, FILEGROWTH = 128MB )
 LOG ON 
( NAME = N'UsersDb_log', FILENAME = N'D:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\UsersDb_log.ldf', SIZE = 64MB , MAXSIZE = 2048GB , FILEGROWTH = 64MB )
;
GO

ALTER DATABASE [UsersDb] SET COMPATIBILITY_LEVEL = 130
GO

IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
	EXEC [UsersDb].[dbo].[sp_fulltext_database] @action = 'enable';
end
GO

ALTER DATABASE [UsersDb] SET RECOVERY SIMPLE;
GO
ALTER DATABASE [UsersDb] SET  MULTI_USER;
GO
ALTER DATABASE [UsersDb] SET READ_WRITE;
GO
ALTER DATABASE [UsersDb] SET QUERY_STORE = ON;
GO


USE [UsersDb];
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

create table [data].[Users]
(
	[UserGuid] [uniqueidentifier] constraint [PK_data_Users_UserGuid] primary key nonclustered not null,
	[UserName] [nvarchar](50) null,
	[FirstName] [nvarchar](50) null,
	[LastName] [nvarchar](50) null,
	[EMail] [nvarchar](50) null,
	[DateCreated] [datetime2] null,
	[DateUpdated] [datetime2] null
)
on [primary];
go

alter table [data].[Users] add constraint [DF_data_Users_UserGuid] default (newsequentialid()) for [UserGuid];
alter table [data].[Users] add constraint [DF_data_Users_DateCreated] default (getutcdate()) for [DateCreated];
alter table [data].[Users] add constraint [DF_data_Users_DateUpdated] default (getutcdate()) for [DateUpdated];
go

-- ENABLE CDC for table
exec sys.sp_cdc_enable_table
	@source_schema = 'data',
	@source_name = 'Users',
	@role_name = 'ETLRole',
	@supports_net_changes = 1,
	@index_name = 'PK_data_Users_UserGuid',
	@captured_column_list = null
;
go


create table [etl].[Changes]
(
	[CaptureInstance] [nvarchar](300) constraint [PK_etl_Changes_CaptureInstance] primary key nonclustered not null,
	[Lsn] [binary](10) not null
)
on [primary];
go

-- END TABLES
-- ==================================================


-- ==================================================
-- BEGIN STORED PROCEDURES

-- BEGIN ENTITY-SPECIFIC DML

create proc [data].[CreateUser]
	@UserName		nvarchar(50),
	@FirstName		nvarchar(50),
	@LastName		nvarchar(50),
	@EMail			nvarchar(50),
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
		EMail
	)
	values
	(
		@UserGuid,
		@UserName,
		@FirstName,
		@LastName,
		@EMail
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
	@EMail			nvarchar(50)
as
begin
	update
		[data].[Users]
	set
		[EMail] = @EMail,
		[DateUpdated] = getutcdate()
	where
		[UserGuid] = @UserGuid
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

create proc [etl].[GetChangesUsers]
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

	set		@captureInstance = 'data_Users';
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
			[EventData] = JSON_QUERY((select UserGuid, UserName, FirstName, LastName, EMail, DateCreated, DateUpdated for json path, without_array_wrapper))
		from
			cdc.fn_cdc_get_all_changes_data_Users(@from_lsn, @to_lsn, @scope) cdc
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
