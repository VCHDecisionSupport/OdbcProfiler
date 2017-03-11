
DECLARE @ServerName VARCHAR(100) = 'sname'
    ,@ServerType VARCHAR(100) = 'stype'
    ,@DatabaseName VARCHAR(100) = 'dbname'
    ,@PhysicalViewTableName VARCHAR(500) = 'vtname'
    ,@RowCount INT = 1235
    ,@ViewTableProfileId INT
    ,@ProfileIsoDateStr VARCHAR(23) = '2012-01-06T16:02:52.658'
EXEC dbo.uspInsViewTableProfile 
	@ServerName = @ServerName
    ,@ServerType = @ServerType
    ,@DatabaseName = @DatabaseName
    ,@PhysicalViewTableName = @PhysicalViewTableName
    ,@RowCount = @RowCount
	,@ViewTableProfileId = @ViewTableProfileId OUTPUT
	,@ProfileIsoDateStr = @ProfileIsoDateStr


TRUNCATE TABLE GenericProfiles.dbo.ViewTableProfile;
TRUNCATE TABLE GenericProfiles.dbo.ViewTableInfo;
TRUNCATE TABLE GenericProfiles.dbo.DatabaseInfo;
TRUNCATE TABLE GenericProfiles.dbo.ServerInfo;
SELECT * FROM GenericProfiles.dbo.ServerInfo;
SELECT * FROM GenericProfiles.dbo.DatabaseInfo;
SELECT * FROM GenericProfiles.dbo.ViewTableInfo;
SELECT * FROM GenericProfiles.dbo.ViewTableProfile;
SELECT * FROM GenericProfiles.dbo.vwViewTableInfo;
SELECT * FROM GenericProfiles.dbo.vwViewTableProfile;
SELECT @ViewTableProfileId AS ViewTableProfileId;




DECLARE @ServerName VARCHAR(100) = 'sname'
    ,@ServerType VARCHAR(100) = 'stype'
    ,@DatabaseName VARCHAR(100) = 'dbname'
    ,@PhysicalViewTableName VARCHAR(500) = 'vtname'
    ,@RowCount INT = 1235
    ,@ViewTableInfoId INT
    ,@ProfileIsoDateStr VARCHAR(23) = '2012-01-06T16:02:52.658'
EXEC dbo.uspInsViewTableInfo @ServerName = @ServerName
    ,@ServerType = @ServerType
    ,@DatabaseName = @DatabaseName
    ,@PhysicalViewTableName = @PhysicalViewTableName
	,@ViewTableInfoId = @ViewTableInfoId OUTPUT
	,@Debug = 1


TRUNCATE TABLE GenericProfiles.dbo.ServerInfo;
TRUNCATE TABLE GenericProfiles.dbo.DatabaseInfo;
TRUNCATE TABLE GenericProfiles.dbo.ViewTableInfo;
TRUNCATE TABLE GenericProfiles.dbo.ViewTableProfile;
SELECT * FROM GenericProfiles.dbo.ServerInfo;
SELECT * FROM GenericProfiles.dbo.DatabaseInfo;
SELECT * FROM GenericProfiles.dbo.ViewTableInfo;
SELECT * FROM GenericProfiles.dbo.ViewTableProfile;
SELECT @ViewTableInfoId AS ViewTableInfoId;