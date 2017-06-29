use [TestDb];
go

create schema [data];
go

create table [data].[Samples]
(
	[SampleGuid] [uniqueidentifier] constraint [PK_data_Samples_SampleGuid] primary key nonclustered not null,
	[Sample] [nvarchar](max) null,
	[DateCreated] [datetime2] null,
	[DateUpdated] [datetime2] null
)
on [primary];
go

alter table [data].[Samples] add constraint [DF_data_Samples_SampleGuid] default (newsequentialid()) for [SampleGuid];
alter table [data].[Samples] add constraint [DF_data_Samples_DateCreated] default (getutcdate()) for [DateCreated];
alter table [data].[Samples] add constraint [DF_data_Samples_DateUpdated] default (getutcdate()) for [DateUpdated];
go


create table [data].[Transactions]
(
	TransactionID			varchar(100) null,
	PolicyManuscriptID		varchar(200) null,
	[Type]					varchar(20) null,
	[Status]				varchar(20) null,
	EffectiveDate			datetime2 null,
	CreatedDate				datetime2 null,
	CreatedUser				varchar(50) null,
	Charge					decimal null,
	HistoryID				bigint null,
	ExpirationDate			datetime2 null,
	PolicyStatus			varchar(20) null,
	IssuedDate				datetime2 null,
	[DateCreated]			[datetime2] null,
	[DateUpdated]			[datetime2] null,
	SliceId					binary(32) null
)
on [primary];
go

alter table [data].[Transactions] add constraint [DF_data_Transactions_DateCreated] default (getutcdate()) for [DateCreated];
alter table [data].[Transactions] add constraint [DF_data_Transactions_DateUpdated] default (getutcdate()) for [DateUpdated];
go

create table [data].[TransactionReasons]
(
	TransactionID			varchar(100) null,
	ReasonID				varchar(100) null,
	ReasonCode				varchar(20) null,
	FirstPositionOfCode		varchar(20) null,
	SliceId					binary(32) null
)
on [primary];
go

create table [data].[SessionProperties]
(
	TransactionID			varchar(100) null,
	SessionID				varchar(100) null,
	DateModified			datetime2 null,
	Manuscript				varchar(100) null,
	CultureCode				varchar(20) null,
	PropertiesID			varchar(100) null,
	SliceId					binary(32) null
)
on [primary];
go

create table [data].[Policy]
(
	TransactionID			varchar(100) null,
	PolicyId				varchar(100) null,
	QuoteNumber				varchar(100) null,
	LineOfBusiness			varchar(100) null,
	PrimaryRatingState		varchar(20) null,
	PolicyNumber			varchar(100) null,
	EffectiveDate			datetime2 null,
	Product					varchar(100) null,
	[Status]				varchar(20) null,
	Term					varchar(20) null,
	ExpirationDate			datetime2 null,
	SliceId					binary(32) null
)
on [primary];
go

create table [data].[Account]
(
	TransactionID			varchar(100) null,
	AccountID				varchar(100) null,
	AccountIDSub			varchar(100) null,
	AccountName				varchar(100) null,
	AccountAddress			nvarchar(max) null,
	SliceId					binary(32) null
)
on [primary];
go

create table [data].[ExternalCalls]
(
	TransactionID			varchar(100) null,
	ExternalCalls			nvarchar(max) null,
	SliceId					binary(32) null
)
on [primary];
go


