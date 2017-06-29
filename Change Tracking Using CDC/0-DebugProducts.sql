use [ProductsDb];

declare @productCategoryGuid01 uniqueidentifier;

exec data.CreateProductCategory 'Household', @productCategoryGuid01 output;
exec data.CreateProductCategory 'Learning';
exec data.CreateProductCategory 'Food';
exec data.CreateProductCategory 'Automotive';
exec data.CreateProductCategory 'Music';
exec data.CreateProductCategory 'Movies';
exec data.CreateProductCategory 'Parenting';
exec data.CreateProductCategory 'Books';
exec data.CreateProductCategory 'Cleaning';
exec data.CreateProductCategory 'Clothing';

waitfor delay '00:00:05';

exec etl.GetChangesProductCategories;

waitfor delay '00:00:05';

exec data.UpdateProductCategory @productCategoryGuid01, 'Haushalt';
exec data.CreateProductCategory 'Cartoons';
exec data.CreateProductCategory 'Comics';
exec data.CreateProductCategory 'Electronics';

waitfor delay '00:00:05';

exec etl.GetChangesProductCategories;

exec data.DeleteProductCategory @productCategoryGuid01;

waitfor delay '00:00:05';

exec etl.GetChangesProductCategories;