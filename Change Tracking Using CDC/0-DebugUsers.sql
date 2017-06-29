use [UsersDb];

declare @guid uniqueidentifier;

exec data.CreateUser 'pelazem', 'Patrick', 'El-Azem', 'p@elazem.com', @guid output;
exec data.UpdateUser @guid, 'paelaz@microsoft.com';

waitfor delay '00:00:05';

exec etl.GetChangesUsers;

waitfor delay '00:00:05';

exec data.DeleteUser @guid;

waitfor delay '00:00:05';

exec etl.GetChangesUsers;

waitfor delay '00:00:05';

exec data.DeleteUser @guid;

waitfor delay '00:00:05';

exec etl.GetChangesUsers;
