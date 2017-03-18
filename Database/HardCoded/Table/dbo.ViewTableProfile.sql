USE GenericProfiles
GO

IF OBJECT_ID('dbo.ViewTableProfile') IS NOT NULL
BEGIN
    DROP TABLE dbo.ViewTableProfile;
END

RAISERROR('CREATE TABLE dbo.ViewTableProfile', 1, 1) WITH NOWAIT;

CREATE TABLE dbo.ViewTableProfile
(
    ViewTableProfileID INT IDENTITY(1,1)
    ,ViewTableInfoID INT NOT NULL
    ,ProfileDate DATETIME
    ,ViewTableRowCount INT
	,CONSTRAINT PK_ViewTableProfile PRIMARY KEY NONCLUSTERED (ViewTableProfileID)
	,CONSTRAINT CI_ViewTableProfile UNIQUE CLUSTERED (ProfileDate, ViewTableProfileID)
)
GO

ALTER TABLE dbo.ViewTableProfile     
ADD CONSTRAINT FK_ViewTableProfile_ViewTableInfo FOREIGN KEY (ViewTableInfoID)     
    REFERENCES dbo.ViewTableInfo (ViewTableInfoID)     
    ON DELETE CASCADE    
    ON UPDATE CASCADE    
;    