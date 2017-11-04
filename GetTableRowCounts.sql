set nocount off;

declare	@TableName varchar(250),
		@TableSql varchar(300);

declare TableCursor cursor fast_forward for
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

open TableCursor;

fetch next from TableCursor into @TableName;

while @@FETCH_STATUS = 0
begin
	select	@TableSql = 'select TableName = ''' + @TableName + ''', NumRows = count(*) from ' + @TableName + ';';

	exec sp_sqlexec @TableSql;

	fetch next from TableCursor into @TableName;
end

close TableCursor;
deallocate TableCursor;
go
