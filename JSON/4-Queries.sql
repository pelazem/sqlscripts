select * from data.Account;
select * from data.ExternalCalls;
select * from data.[Policy];
select * from data.SessionProperties;
select * from data.TransactionReasons;
select * from data.Transactions;
go


select
	TransactionID,
	AccountID,
	AccountIDSub,
	AccountName,
	JSON_VALUE(AccountAddress, '$.address.id') as [AddressId],
	AccountAddress
from
	data.Account;
go
	
	
-- See https://docs.microsoft.com/en-us/sql/relational-databases/json/json-data-sql-server

select * from data.Samples;

select
	SampleGuid,
	JSON_VALUE(Sample, '$.id') as [Id],
	JSON_QUERY(Sample, '$.site.regions') as [Regions]
from
	data.Samples;
go