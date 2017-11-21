-- Given a column NAME in a table, returns the column NUMBER
-- Note that column NUMBER is 1-based

create function [data].[GetColumnNumberForColumnName]
(
	@SchemaName		varchar(50),
	@TableName		varchar(50),
	@ColumnName		varchar(50)
)
returns int
as
begin
	declare	@result int;

	select
		@result = c.column_id
	from
		sys.tables t
		inner join sys.schemas s on t.schema_id = s.schema_id
		inner join sys.columns c on t.object_id = c.object_id
	where
		s.name = @SchemaName and
		t.name = @TableName and
		c.name = @ColumnName
	;

	return	@result;
end
go