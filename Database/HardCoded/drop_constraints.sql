USE GenericProfiles
GO

DECLARE @sql nvarchar(max);
DECLARE @params nvarchar(max);

DECLARE fk_cur CURSOR
FOR
SELECT tab.name AS TableName, fk.name AS ForeignKeyName
FROM sys.foreign_keys AS fk
JOIN sys.tables AS tab
ON fk.parent_object_id = tab.object_id
WHERE tab.name NOT LIKE '%sys%';

OPEN fk_cur;

DECLARE @tb_name varchar(128);
DECLARE @fk_name varchar(128);

FETCH NEXT FROM fk_cur INTO @tb_name, @fk_name;

WHILE @@FETCH_STATUS = 0
BEGIN
	SELECT @tb_name, @fk_name;

	SET @sql = 'ALTER TABLE '+@tb_name +' DROP CONSTRAINT '+ @fk_name;
	PRINT @sql
	EXEC(@sql);
	FETCH NEXT FROM fk_cur INTO @tb_name, @fk_name;
END
GO
CLOSE fk_cur;
DEALLOCATE fk_cur;
GO

DECLARE @sql nvarchar(max);
DECLARE @params nvarchar(max);

DECLARE kc_cur CURSOR
FOR
SELECT tab.name AS TableName, kc.name AS IndexName
FROM sys.key_constraints AS kc
JOIN sys.tables AS tab
ON kc.parent_object_id = tab.object_id
WHERE tab.name NOT LIKE '%sys%';

OPEN kc_cur;

DECLARE @tb_name varchar(128);
DECLARE @kc_name varchar(128);

FETCH NEXT FROM kc_cur INTO @tb_name, @kc_name;

WHILE @@FETCH_STATUS = 0
BEGIN
	SELECT @tb_name, @kc_name;

	SET @sql = 'ALTER TABLE '+@tb_name +' DROP CONSTRAINT '+ @kc_name;
	PRINT @sql
	EXEC(@sql);
	FETCH NEXT FROM kc_cur INTO @tb_name, @kc_name;
END
CLOSE kc_cur;
DEALLOCATE kc_cur;
GO

DECLARE @sql nvarchar(max);
DECLARE @params nvarchar(max);

DECLARE ix_cur CURSOR
FOR
SELECT tab.name AS TableName, idx.name AS IndexName
FROM sys.indexes AS idx
JOIN sys.tables AS tab
ON idx.object_id = tab.object_id
WHERE tab.name NOT LIKE '%sys%';

OPEN ix_cur;

DECLARE @tb_name varchar(128);
DECLARE @ix_name varchar(128);

FETCH NEXT FROM ix_cur INTO @tb_name, @ix_name;

WHILE @@FETCH_STATUS = 0
BEGIN
	SELECT @tb_name, @ix_name;

	SET @sql = ' DROP INDEX '+ @ix_name+' ON '+@tb_name;
	PRINT @sql
	EXEC(@sql);
	FETCH NEXT FROM ix_cur INTO @tb_name, @ix_name;
END
CLOSE ix_cur;
DEALLOCATE ix_cur;
GO
