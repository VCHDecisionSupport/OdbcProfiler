USE AutoTest
GO

CREATE VIEW prod_vs_beta
AS

WITH serverA AS (
	SELECT 
		serv.server_name
		,serv.server_type
		,serv.server_info_id
		,db.database_name
		,db.database_info_id
		,tab.ansi_view_table_name
		,tab.view_table_info_id
		,tabpro.view_table_row_count_date
		,tabpro.view_table_row_count
		,tabpro.view_table_row_count_execution_time
	FROM [AutoTest].[dbo].[server_info] AS serv
	LEFT JOIN [AutoTest].[dbo].database_info AS db
	ON serv.server_info_id = db.server_info_id
	LEFT JOIN [AutoTest].[dbo].view_table_info AS tab
	ON db.database_info_id = tab.database_info_id
	LEFT JOIN [AutoTest].[dbo].view_table_profile AS tabpro
	ON tabpro.view_table_info_id = tab.view_table_info_id
	LEFT JOIN [AutoTest].[dbo].column_info AS col
	ON col.view_table_info_id = tab.view_table_info_id
), serverB AS (
	SELECT 
		serv.server_name
		,serv.server_type
		,serv.server_info_id
		,db.database_name
		,db.database_info_id
		,tab.ansi_view_table_name
		,tab.view_table_info_id
		,tabpro.view_table_row_count_date
		,tabpro.view_table_row_count
		,tabpro.view_table_row_count_execution_time
	FROM [AutoTest].[dbo].[server_info] AS serv
	LEFT JOIN [AutoTest].[dbo].database_info AS db
	ON serv.server_info_id = db.server_info_id
	LEFT JOIN [AutoTest].[dbo].view_table_info AS tab
	ON db.database_info_id = tab.database_info_id
	LEFT JOIN [AutoTest].[dbo].view_table_profile AS tabpro
	ON tabpro.view_table_info_id = tab.view_table_info_id
	LEFT JOIN [AutoTest].[dbo].column_info AS col
	ON col.view_table_info_id = tab.view_table_info_id
)
SELECT 
	serverA.database_name
	,serverA.ansi_view_table_name
	,serverA.server_name AS serverA_server_name
	,serverA.server_type AS serverA_server_type
	,serverA.view_table_row_count_execution_time AS serverA_view_table_row_count_execution_time
	,serverA.view_table_row_count AS serverA_view_table_row_count
	,serverB.view_table_row_count AS serverB_view_table_row_count
	,serverB.view_table_row_count_execution_time AS serverB_view_table_row_count_execution_time
	,serverB.server_type AS serverB_server_type
	,serverB.server_name AS serverB_server_name
FROM serverA
FULL JOIN serverB
ON serverA.ansi_view_table_name = serverB.ansi_view_table_name
AND serverA.server_name = 'STDBDECSUP03'
AND serverB.server_name = 'SPDBDECSUP04'