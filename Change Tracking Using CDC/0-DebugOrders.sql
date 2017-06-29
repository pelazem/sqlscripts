use [OrdersDb];

declare @userGuid uniqueidentifier, @order01Guid uniqueidentifier;
select @userGuid = newid();

exec data.CreateOrder @userGuid, 'Order01', @order01Guid output;
exec data.CreateOrder @userGuid, 'Order02';
exec data.CreateOrder @userGuid, 'Order03';
exec data.CreateOrder @userGuid, 'Order04';
exec data.CreateOrder @userGuid, 'Order05';
exec data.CreateOrder @userGuid, 'Order06';
exec data.CreateOrder @userGuid, 'Order07';
exec data.CreateOrder @userGuid, 'Order08';

waitfor delay '00:00:05';

exec etl.GetChangesOrders;

waitfor delay '00:00:05';

exec data.UpdateOrder @order01Guid, 'Order01Updated';
exec data.CreateOrder @userGuid, 'Order01';
exec data.CreateOrder @userGuid, 'Order02';
exec data.CreateOrder @userGuid, 'Order03';
exec data.CreateOrder @userGuid, 'Order04';

waitfor delay '00:00:05';

exec etl.GetChangesOrders;

waitfor delay '00:00:05';

exec data.DeleteOrder @order01Guid;

waitfor delay '00:00:05';

exec etl.GetChangesOrders;