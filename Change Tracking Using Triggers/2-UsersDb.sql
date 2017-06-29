USE [master];
GO

CREATE DATABASE [UsersDb]
	CONTAINMENT = NONE
	ON PRIMARY 
( NAME = N'UsersDb', FILENAME = N'D:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\UsersDb.mdf' , SIZE = 128MB , MAXSIZE = UNLIMITED, FILEGROWTH = 128MB )
 LOG ON 
( NAME = N'UsersDb_log', FILENAME = N'D:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\UsersDb_log.ldf' , SIZE = 64MB , MAXSIZE = 2048GB , FILEGROWTH = 64MB )
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

create table [data].[Users]
(
	[UserGuid] [uniqueidentifier] not null,
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
alter table [data].[Users] add constraint [PK_data_Users_UserGuid] primary key nonclustered (UserGuid);
go



create table [etl].[ChangesUsers]
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

alter table [etl].[ChangesUsers] add constraint [DF_etl_ChangesUsers_ChangeGuid] default (newsequentialid()) for [ChangeGuid];
alter table [etl].[ChangesUsers] add constraint [DF_etl_ChangesUsers_EventDateTime] default (getutcdate()) for [EventDateTime];
go
alter table [etl].[ChangesUsers] add constraint [PK_etl_ChangesUsers_ChangeGuid] primary key nonclustered (ChangeGuid);
go

CREATE NONCLUSTERED INDEX [ixEventDateTime] ON [etl].[ChangesUsers]
(
	[EventDateTime] ASC
)
INCLUDE
(
	[ChangeGuid]
)
WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF);
go

CREATE CLUSTERED INDEX [ixcBatchGuid] ON [etl].[ChangesUsers]
(
	[BatchGuid] ASC
)
WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF);
go

-- END TABLES
-- ==================================================


-- ==================================================
-- BEGIN TRIGGERS

create trigger [data].[Delete_data_Users]
on [data].[Users]
after delete
as
begin
	set nocount on;

	insert into
		[etl].[ChangesUsers]
		(
			[EventType],
			[DataItemSource],
			[DataItemGuid],
			[EventData]
		)
	select
		'DELETE',
		'data.Users',
		[UserGuid],
		JSON_QUERY((select UserGuid, UserName, FirstName, LastName, EMail, DateCreated, DateUpdated for json path, without_array_wrapper))
	from
		deleted;
end
go

create trigger [data].[Create_data_Users]
on [data].[Users]
after insert
as
begin
	set nocount on;

	insert into
		[etl].[ChangesUsers]
		(
			[EventType],
			[DataItemSource],
			[DataItemGuid],
			[EventData]
		)
	select
		'CREATE',
		'data.Users',
		[UserGuid],
		JSON_QUERY((select UserGuid, UserName, FirstName, LastName, EMail, DateCreated, DateUpdated for json path, without_array_wrapper))
	from
		inserted;
end
go

create trigger [data].[Update_data_Users]
on [data].[Users]
after update
as
begin
	set nocount on;

	insert into
		[etl].[ChangesUsers]
		(
			[EventType],
			[DataItemSource],
			[DataItemGuid],
			[EventData]
		)
	select
		'UPDATE',
		'data.Users',
		[UserGuid],
		JSON_QUERY((select UserGuid, UserName, FirstName, LastName, EMail, DateCreated, DateUpdated for json path, without_array_wrapper))
	from
		inserted;
end
go

-- END TRIGGERS
-- ==================================================


-- ==================================================
-- BEGIN STORED PROCEDURES

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

create proc [etl].[GetChangesUsers]
	@batchSize	int = 1000
as
begin
	declare	@BatchGuid	uniqueidentifier;
	select	@BatchGuid = newid();

	-- PREP BATCH
	update
		[etl].[ChangesUsers]
	set
		[BatchGuid] = @BatchGuid
	where
		[ChangeGuid] in
		(
			select
				[ChangeGuid]
			from
				[etl].[ChangesUsers]
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
		[etl].[ChangesUsers]
	where
		[BatchGuid] = @BatchGuid
	;	

	-- CLEANUP
	delete from
		[etl].[ChangesUsers]
	where
		[BatchGuid] = @BatchGuid
end
go

-- END STORED PROCEDURES
-- ==================================================

