set nocount off;

declare	@TableName varchar(250),
		@TableSql varchar(300);

declare TableCleanup cursor fast_forward for
	select
		'[' + s.name + '].[' + t.name + ']' as FQTableName
	from
		sys.tables t
		inner join sys.schemas s on t.schema_id = s.schema_id
	where
		t.type = 'U'
	order by
		s.name,
		t.name
;

open TableCleanup;

fetch next from TableCleanup into @TableName;

while @@FETCH_STATUS = 0
begin
	--select	@TableSql = 'truncate table ' + @TableName + ';';
	select	@TableSql = 'delete from ' + @TableName + ';';

	exec sp_sqlexec @TableSql;

	fetch next from TableCleanup into @TableName;
end

close TableCleanup;
deallocate TableCleanup;
go
