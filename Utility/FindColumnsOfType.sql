declare @TypeName varchar(20);

select	@TypeName = 'xml';

select
	'[' + s.name + '].[' + t.name + ']' as FQTableName,
	c.name as ColName,
	ty.name as TypeName
from
	sys.tables t
	inner join sys.schemas s on t.schema_id = s.schema_id
	inner join sys.columns c on t.object_id = c.object_id
	inner join sys.types ty on c.system_type_id = ty.system_type_id
where
	t.type = 'U' and
	ty.name = @TypeName
order by
	s.name,
	t.name,
	c.column_id
;
