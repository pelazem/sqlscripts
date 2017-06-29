use [TestDb];
go

-- Users
alter table	data.Users
alter column [EMail] drop masked;

alter table	data.Users
alter column [SSN] drop masked;

alter table data.Users
alter column [DoB] drop masked;

-- Orders
alter table data.Orders
alter column [CreditCardNumber] drop masked;

alter table data.Orders
alter column [CreditCardExp] drop masked;

alter table data.Orders
alter column [CreditCardSecCode] drop masked;
