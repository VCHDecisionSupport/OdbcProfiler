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
	,CONSTRAINT PK_DatabaseInfo PRIMARY KEY NONCLUSTERED (DatabaseInfoID)
	,CONSTRAINT CI_DatabaseInfo UNIQUE CLUSTERED (DatabaseName)
)
GO

ALTER TABLE dbo.DatabaseInfo     
ADD CONSTRAINT FK_DatabaseInfo_ServerInfo FOREIGN KEY (ServerInfoID)     
    REFERENCES dbo.ServerInfo (ServerInfoID)     
    ON DELETE CASCADE    
    ON UPDATE CASCADE    
;    