-- Given a column NUMBER in a table, returns the column NAME
-- Note that column NUMBER is 1-based

create function [data].[GetColumnNameForColumnNumber]
(
	@SchemaName		varchar(50),
	@TableName		varchar(50),
	@ColumnNumber	int
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
		c.column_id = @ColumnNumber
	;

	return	@result;
end
go