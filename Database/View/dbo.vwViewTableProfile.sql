USE GenericProfiles
GO

IF  NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'dbo.vwViewTableInfo') AND OBJECTPROPERTY(id,N'IsProcedure') = 1)
BEGIN
    EXEC ('CREATE VIEW dbo.vwViewTableInfo AS SELECT 1 AS [one];');
END
GO

ALTER VIEW dbo.vwViewTableInfo
AS
SELECT 
	serv.ServerType
	,serv.ServerName
	,db.DatabaseName
	,tab.PhysicalViewTableName
FROM dbo.ServerInfo AS serv
JOIN dbo.DatabaseInfo AS db
ON serv.ServerInfoID = db.ServerInfoID
JOIN dbo.ViewTableInfo AS tab
ON db.DatabaseInfoID = tab.DatabaseInfoID

GO