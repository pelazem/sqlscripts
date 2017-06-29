use [TestDb];
go

-- ==================================================
-- BEGIN ALTER TABLES/COLUMNS TO ADD DDM

-- Users
alter table	data.Users
alter column [EMail] add masked with (function = 'email()');

alter table	data.Users
alter column [SSN] add masked with (function = 'partial(0,"XXX-XX-",4)');

alter table data.Users
alter column [DoB] add masked with (function = 'default()');

-- Orders
alter table data.Orders
alter column [CreditCardNumber] add masked with (function = 'partial(0, "XXXX-XXXX-XXXX-", 4)');

alter table data.Orders
alter column [CreditCardExp] add masked with (function = 'partial(0, "X", 2)');

alter table data.Orders
alter column [CreditCardSecCode] add masked with (function = 'default()');

-- END ALTER TABLES/COLUMNS TO ADD DDM
-- ==================================================
