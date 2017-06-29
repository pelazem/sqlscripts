USE [master];
GO

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'MyStrong2017Password@@';
go

CREATE CERTIFICATE MyTestTDECert WITH SUBJECT = 'My Test TDE Certificate';
go

-- Cert backups written to file system on SQL Server - NOT on the machine where I am working in SSMS
BACKUP CERTIFICATE MyTestTDECert TO FILE = N'c:\temp\MyTestTDECert.crt'
WITH PRIVATE KEY   
(   
	FILE = N'c:\temp\MyTestTDECert.ppk' ,  
	ENCRYPTION BY PASSWORD = 'MyStrong2017Password@@'   
)   
;  
go

USE [TestDb];
GO

CREATE DATABASE ENCRYPTION KEY
WITH ALGORITHM = AES_128
ENCRYPTION BY SERVER CERTIFICATE MyTestTDECert;
GO

ALTER DATABASE TestDb
SET ENCRYPTION ON;
GO
