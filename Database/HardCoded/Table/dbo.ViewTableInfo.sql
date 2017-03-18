USE GenericProfiles
GO

IF OBJECT_ID('dbo.ViewTableInfo') IS NOT NULL
BEGIN
    DROP TABLE dbo.ViewTableInfo;
END

RAISERROR('CREATE TABLE dbo.ViewTableInfo', 1, 1) WITH NOWAIT;

CREATE TABLE dbo.ViewTableInfo
(
    ViewTableInfoID INT IDENTITY(1,1)
    ,DatabaseInfoID INT NOT NULL
    ,PhysicalViewTableName VARCHAR(100) NOT NULL-- if sql: "database_name"."schema_name"."table_name" -- if dendoo "database_name"."table_name"
    ,LogicalViewTablePath VARCHAR(100) -- if sql: "database_name"."schema_name"."table_name" -- if dendoo "database_name"/"root_folder"/"folder"/.../"parent_folder"/"table_name"
	,CONSTRAINT PK_ViewTableInfo PRIMARY KEY NONCLUSTERED (ViewTableInfoID)
	,CONSTRAINT CI_ViewTableInfo UNIQUE CLUSTERED (PhysicalViewTableName)
)
GO

ALTER TABLE dbo.ViewTableInfo     
ADD CONSTRAINT FK_ViewTableInfo_DatabaseInfo FOREIGN KEY (DatabaseInfoID)     
    REFERENCES dbo.DatabaseInfo (DatabaseInfoID)     
    ON DELETE CASCADE    
    ON UPDATE CASCADE    
;    