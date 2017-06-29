USE [master];
GO

drop database if exists [TestDb];
go

CREATE DATABASE [TestDb]
	CONTAINMENT = NONE
	ON PRIMARY 
( NAME = N'TestDb', FILENAME = N'E:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\TestDb.mdf', SIZE = 64MB , MAXSIZE = UNLIMITED, FILEGROWTH = 64MB )
 LOG ON 
( NAME = N'TestDb_log', FILENAME = N'E:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\TestDb_log.ldf', SIZE = 32MB , MAXSIZE = 2048GB , FILEGROWTH = 32MB )
;
GO

ALTER DATABASE [TestDb] SET COMPATIBILITY_LEVEL = 130
GO

IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
	EXEC [TestDb].[dbo].[sp_fulltext_database] @action = 'enable';
end
GO

ALTER DATABASE [TestDb] SET CONTAINMENT = PARTIAL;
go
ALTER DATABASE [TestDb] SET RECOVERY SIMPLE;
GO
ALTER DATABASE [TestDb] SET  MULTI_USER;
GO
ALTER DATABASE [TestDb] SET READ_WRITE;
GO
ALTER DATABASE [TestDb] SET QUERY_STORE = ON;
GO

USE [TestDb];
go

