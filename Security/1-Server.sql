use master;
go

sp_configure 'contained database authentication', 1;
GO
RECONFIGURE;
GO
