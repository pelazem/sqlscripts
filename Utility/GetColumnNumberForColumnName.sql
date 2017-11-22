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

	with ColCTE as
	(
		select
			c.name as ColumnName,
			ROW_NUMBER() over (order by c.column_id) as ColumnNumber
		from
			sys.tables t
			inner join sys.schemas s on t.schema_id = s.schema_id
			inner join sys.columns c on t.object_id = c.object_id
		where
			s.name = @SchemaName and
			t.name = @TableName
	)
	select
		@result = ColumnNumber
	from
		ColCTE
	where
		ColumnName = @ColumnName
	;

	return	@result;
end
go