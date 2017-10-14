USE [TestDb];
go


-- ==================================================
-- Schemas

create schema [data];
go
create schema [ref];
go
-- ==================================================


-- ==================================================
-- Tables - Reference

create table [ref].[Users]
(
	[UserId] [int] identity(1,1) primary key not null,
	[UserGuid] [uniqueidentifier] not null,
	[UserName] [nvarchar](50) not null,
	[Email] [nvarchar](50) null,
	[FirstName] [nvarchar](50) null,
	[LastName] [nvarchar](50) null,
	[DateCreated] [datetime2] null,
	[DateUpdated] [datetime2] null
)    
ON [PRIMARY];
go
alter table [ref].[Users] add constraint [DF_ref_Users_UserGuid] default (newsequentialid()) for [UserGuid];
go

create table [ref].[Products]
(
	[ProductId] [int] identity(1,1) primary key not null,
	[ProductGuid] [uniqueidentifier] not null,
	[ProductName] [nvarchar](50) not null,
	[Sku] [nvarchar](50) null,
	[Packaging] [nvarchar](50) null,
	[DateCreated] [datetime2] null,
	[DateUpdated] [datetime2] null
)    
ON [PRIMARY];
go
alter table [ref].[Products] add constraint [DF_ref_Products_ProductGuid] default (newsequentialid()) for [ProductGuid];
go
-- ==================================================


-- ==================================================
-- Tables - Data

create table [data].[Orders]
(
	[OrderId] [int] identity(1,1) primary key not null,
	[UserId] [int] not null,
	[OrderGuid] [uniqueidentifier] not null,
	[OrderName] [nvarchar](50) not null,
	[InvoiceNumber] [nvarchar](50) null,
	[PONumber] [nvarchar](50) null,
	[DateCreated] [datetime2] null,
	[DateUpdated] [datetime2] null
)    
ON [PRIMARY];
go
alter table [data].[Orders] add constraint [DF_data_Orders_OrderGuid] default (newsequentialid()) for [OrderGuid];
go

create table [data].[OrderLines]
(
	[OrderLineId] [int] identity(1,1) primary key not null,
	[OrderLineGuid] [uniqueidentifier] not null,
	[OrderId] [int] not null,
	[ProductId] [int] not null,
	[Qty] [numeric](18,5) not null,
	[UnitPrice] [numeric](18,5) not null,
	[Discount] [numeric](18,5) not null,
	[DateCreated] [datetime2] null,
	[DateUpdated] [datetime2] null
)    
ON [PRIMARY];
go
alter table [data].[OrderLines] add constraint [DF_data_OrderLines_OrderLineGuid] default (newsequentialid()) for [OrderLineGuid];
go
-- ==================================================


-- ==================================================
-- Tables - XML

create table [data].[XmlBlobs]
(
	[TransactionGuid] [uniqueidentifier] not null,
	[Symbol] [nvarchar](10) null,
	[Amount] [numeric](18,5) null,
	[Settled] [bit] not null,
	[TransactionDate] [datetime2] null,
	[Note] [nvarchar](500) null,
	[XmlComputed] as 
'<xml>
	<TransactionGuid>' + convert(nvarchar(50), TransactionGuid) + '</TransactionGuid>' +
	case when Symbol is not null then '<Symbol>' + Symbol + '</Symbol>' else '' end +
	case when Amount is not null then '<Amount>' + convert(nvarchar(50), Amount) + '</Amount>' else '' end +
	'<Settled>' + convert(nvarchar(10), Settled) + '</Settled>' +
	case when TransactionDate is not null then '<TransactionDate>' + convert(nvarchar(20), TransactionDate) + '</TransactionDate>' else '' end +
	case when Note is not null then '<Note>' + Note + '</Note>' else '' end +
'</xml>'
)
ON [PRIMARY];
go
alter table [data].[XmlBlobs] add constraint [DF_data_XmlBlobs_TransactionGuid] default (newsequentialid()) for [TransactionGuid];
go

-- ==================================================
