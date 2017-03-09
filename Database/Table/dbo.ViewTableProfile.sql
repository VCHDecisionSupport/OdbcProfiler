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
)
GO