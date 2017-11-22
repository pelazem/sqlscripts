create proc	[data].[GetColumns]
	@SchemaName		varchar(50),
	@TableName		varchar(50)
as
begin
	select
		c.column_id,
		c.name,
		ROW_NUMBER() over (order by c.column_id) as ColumnNumber
	from
		sys.tables t
		inner join sys.schemas s on t.schema_id = s.schema_id
		inner join sys.columns c on t.object_id = c.object_id
	where
		s.name = @SchemaName and
		t.name = @TableName
	;
end
go