create function [data].[GetColumnNameForColumnId]
(
	@SchemaName		varchar(50),
	@TableName		varchar(50),
	@ColumnId		int
)
returns varchar(50)
as
begin
	declare	@result varchar(50);

	select
		@result = c.name
	from
		sys.tables t
		inner join sys.schemas s on t.schema_id = s.schema_id
		inner join sys.columns c on t.object_id = c.object_id
	where
		s.name = @SchemaName and
		t.name = @TableName and
		c.column_id = @ColumnId
	;

	return	@result;
end
go