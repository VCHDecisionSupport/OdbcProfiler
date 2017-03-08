USE GenericProfiles
GO

IF OBJECT_ID('dbo.DatabaseInfo') IS NOT NULL
BEGIN
    DROP TABLE dbo.DatabaseInfo;
END

RAISERROR('CREATE TABLE dbo.DatabaseInfo', 1, 1) WITH NOWAIT;

CREATE TABLE dbo.DatabaseInfo
(
    DatabaseInfoID INT IDENTITY(1,1)
    ,ServerInfoID INT NOT NULL
    ,DatabaseName VARCHAR(100)
)
GO