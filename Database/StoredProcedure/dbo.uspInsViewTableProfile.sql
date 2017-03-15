USE GenericProfiles
GO

IF  NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'dbo.uspInsViewTableProfile') AND OBJECTPROPERTY(id,N'IsProcedure') = 1)
BEGIN
    EXEC ('CREATE PROCEDURE dbo.uspInsViewTableProfile AS');
END
GO
ALTER PROCEDURE dbo.uspInsViewTableProfile
    @ServerName VARCHAR(100)
    ,@ServerType VARCHAR(100)
    ,@DatabaseName VARCHAR(100)
    ,@PhysicalViewTableName VARCHAR(500)
    ,@RowCount INT
	-- ,@ViewTableProfileID INT OUTPUT
	,@ProfileIsoDateStr VARCHAR(23) = NULL -- eg. '2012-01-06T16:02:52.658'
	,@Debug bit = 0
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @sql VARCHAR(MAX)
		,@ServerInfoId INT
		,@DatabaseInfoId INT
		,@ViewTableInfoId INT
		,@ViewTableProfileID INT
		,@ViewTableProfileDate DATETIME;
	IF @Debug = 1
	BEGIN
    	SET @sql = 'dbo.uspInsViewTableProfile @ServerName = '''+@ServerName+''', @DatabaseName = '''+@DatabaseName +''', @PhysicalViewTableName = '''+@PhysicalViewTableName +''', @RowCount = '''+CAST(@RowCount AS VARCHAR);
    	PRINT(@sql);
	END

	IF @ProfileIsoDateStr IS NULL
	BEGIN
		SET @ViewTableProfileDate = GETDATE();
	END
	ELSE
	BEGIN
		SET @ViewTableProfileDate = CONVERT(DATETIME, @ProfileIsoDateStr, 126);
	END

	IF @Debug = 1
	BEGIN
    	SELECT @ProfileIsoDateStr AS ProfileIsoDateStr;
	END

	EXEC dbo.uspInsViewTableInfo @ServerName = @ServerName
		,@ServerType = @ServerType
		,@DatabaseName = @DatabaseName
		,@PhysicalViewTableName = @PhysicalViewTableName
		,@ViewTableInfoId = @ViewTableInfoId OUTPUT
		,@Debug = @Debug

	-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
    -- check if @PhysicalViewTableName exists in schema.table.column: dbo.ViewTableInfo.PhysicalViewTableName 
	-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
	INSERT INTO GenericProfiles.dbo.ViewTableProfile (ViewTableInfoID, ProfileDate, ViewTableRowCount)
	SELECT @ViewTableInfoId, @ViewTableProfileDate, @RowCount;
	SET @ViewTableProfileId = IDENT_CURRENT('dbo.ViewTableProfile')
	IF @Debug = 1
	BEGIN
		SELECT @ViewTableProfileId AS ViewTableProfileId;
	END
    RETURN(@ViewTableProfileId)
END
GO

