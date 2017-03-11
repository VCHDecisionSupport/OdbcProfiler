USE GenericProfiles
GO

IF OBJECT_ID('dbo.ServerInfo') IS NOT NULL
BEGIN
    DROP TABLE dbo.ServerInfo;
END

RAISERROR('CREATE TABLE dbo.ServerInfo', 1, 1) WITH NOWAIT;

CREATE TABLE dbo.ServerInfo
(
    ServerInfoID INT IDENTITY(1,1)
    ,ServerName VARCHAR(100)
    ,ServerType VARCHAR(100)
	,CONSTRAINT PK_ServerInfo PRIMARY KEY NONCLUSTERED (ServerInfoID)
	,CONSTRAINT CI_ServerInfo UNIQUE CLUSTERED (ServerName, ServerType)
)
GO