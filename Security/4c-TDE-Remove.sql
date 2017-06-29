USE [TestDb];
GO

ALTER DATABASE TestDb
SET ENCRYPTION OFF;
GO

waitfor delay '00:00:10';

/* Wait for decryption operation to complete, look for a value of  1 in the query below. */
select	encryption_state
from	sys.dm_database_encryption_keys
where	database_id = db_id();
GO

DROP DATABASE ENCRYPTION KEY;
GO

use [master];
go

DROP CERTIFICATE MyTestTDECert;
go

DROP MASTER KEY;
go
