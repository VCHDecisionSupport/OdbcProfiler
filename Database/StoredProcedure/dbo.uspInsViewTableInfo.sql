USE GenericProfiles
GO

IF  NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'dbo.uspInsViewTableInfo') AND OBJECTPROPERTY(id,N'IsProcedure') = 1)
BEGIN
    EXEC ('CREATE PROCEDURE dbo.uspInsViewTableInfo AS');
END
GO
ALTER PROCEDURE dbo.uspInsViewTableInfo
    @ServerName VARCHAR(100)
    ,@ServerType VARCHAR(100)
    ,@DatabaseName VARCHAR(100)
    ,@PhysicalViewTableName VARCHAR(500)
	,@ViewTableInfoID INT OUTPUT
	,@Debug bit = 0
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @sql VARCHAR(MAX)
		,@ServerInfoId INT
		,@DatabaseInfoId INT
		,@ViewTableProfileDate DATETIME;
	IF @Debug = 1
	BEGIN
    	SET @sql = 'dbo.uspInsViewTableInfo @ServerName = '''+@ServerName+''', @DatabaseName = '''+@DatabaseName +''', @PhysicalViewTableName = '''+@PhysicalViewTableName;
    	PRINT(@sql);
	END


	-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
    -- check if @ServerName exists in schema.table.column: dbo.ServerInfo.ServerName 
	-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
	;WITH existing AS (
		SELECT *
		FROM GenericProfiles.dbo.ServerInfo
	)
	MERGE INTO GenericProfiles.dbo.ServerInfo AS in_tab
	USING (SELECT @ServerName, @ServerType) AS in_data (ServerName, ServerType)
	ON (in_data.ServerName = in_tab.ServerName)
	WHEN MATCHED THEN 
		UPDATE SET ServerType = in_tab.ServerType
	WHEN NOT MATCHED THEN 
		INSERT (ServerName, ServerType)
		VALUES (in_data.ServerName, in_data.ServerType)
	--OUTPUT inserted.ServerInfoID INTO @IdsToReturn(ServerId)
	;
	SET @ServerInfoId = IDENT_CURRENT('dbo.ServerInfo');
	IF @Debug = 1
	BEGIN
		SELECT @ServerInfoId AS ServerInfoId;
	END
	-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
	-- check if @DatabaseName exists in schema.table.column: dbo.DatabaseInfo.DatabaseName 
	-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
	;WITH existing AS (
		SELECT *
		FROM GenericProfiles.dbo.DatabaseInfo
	)
	MERGE INTO GenericProfiles.dbo.DatabaseInfo AS in_tab
	USING (SELECT @ServerInfoId, @DatabaseName) AS in_data (ServerInfoId, DatabaseName)
	ON (
		in_data.ServerInfoId = in_tab.ServerInfoId
	AND
		in_data.DatabaseName = in_tab.DatabaseName)
	WHEN MATCHED THEN 
		UPDATE SET DatabaseName = in_tab.DatabaseName
	WHEN NOT MATCHED THEN 
		INSERT (ServerInfoId, DatabaseName)
		VALUES (in_data.ServerInfoId, in_data.DatabaseName)
	--OUTPUT inserted.DatabaseInfoID INTO @IdsToReturn(DatabaseId)
	;
	SET @DatabaseInfoId = IDENT_CURRENT('dbo.DatabaseInfo') 
	IF @Debug = 1
	BEGIN
		SELECT @DatabaseInfoId AS DatabaseInfoId;
	END

	-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
    -- check if @PhysicalViewTableName exists in schema.table.column: dbo.ViewTableInfo.PhysicalViewTableName 
	-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
	;WITH existing AS (
		SELECT *
		FROM GenericProfiles.dbo.ViewTableInfo
	)
	MERGE INTO GenericProfiles.dbo.ViewTableInfo AS in_tab
	USING (SELECT @DatabaseInfoId, @PhysicalViewTableName) AS in_data (DatabaseInfoId, PhysicalViewTableName)
	ON (
		in_data.DatabaseInfoId = in_tab.DatabaseInfoId
	AND
		in_data.PhysicalViewTableName = in_tab.PhysicalViewTableName)
	WHEN MATCHED THEN 
		UPDATE SET PhysicalViewTableName = in_tab.PhysicalViewTableName
	WHEN NOT MATCHED THEN 
		INSERT (DatabaseInfoId, PhysicalViewTableName)
		VALUES (in_data.DatabaseInfoId, in_data.PhysicalViewTableName)
	--OUTPUT inserted.ViewTableInfoID INTO @IdsToReturn(ViewTableId)
	;
	SET @ViewTableInfoId = IDENT_CURRENT('dbo.ViewTableInfo')
	IF @Debug = 1
	BEGIN
		SELECT @ViewTableInfoId AS ViewTableInfoId;
	END

END
GO

