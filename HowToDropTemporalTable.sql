ALTER TABLE [data].[ProductCategories] SET (SYSTEM_VERSIONING = OFF);   
ALTER TABLE [data].[ProductCategories] DROP PERIOD FOR SYSTEM_TIME;
go
drop table [data].[ProductCategories];
drop table [data].[ProductCategoriesHistory];
go
