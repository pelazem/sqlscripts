SELECT
	s.[name] as [Schema],
	t.[name] as [Table],
	c.[name] as [Column],
	c.[system_type_id],
	c.[user_type_id],
	y.[is_user_defined],
	y.[name]
FROM
	sys.tables t
	INNER JOIN sys.schemas s on t.[schema_id] = s.[schema_id]
	INNER JOIN sys.columns c ON t.[object_id] = c.[object_id]
	INNER JOIN sys.types y ON c.[user_type_id] = y.[user_type_id]
WHERE
	y.[name] IN ('geography', 'geometry', 'hierarchyid', 'image', 'ntext', 'numeric', 'sql_variant', 'sysname', 'text', 'timestamp', 'uniqueidentifier', 'xml')
	OR
	(
		y.[name] IN ('varchar', 'varbinary')
		AND
		(
			(c.[max_length] = -1)
			OR
			(c.max_length > 8000)
		)
	)
	OR
	(
		y.[name] IN ('nvarchar')
		AND
		(
			(c.[max_length] = -1)
			OR
			(c.max_length > 4000)
		)
	)
	OR
	y.[is_user_defined] = 1
;