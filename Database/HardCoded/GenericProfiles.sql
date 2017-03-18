USE master
GO

DECLARE @db_name varchar(200) = 'GenericProfiles'

IF DB_ID(@db_name) IS NOT NULL
BEGIN
	ALTER DATABASE GenericProfiles SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE GenericProfiles;
END

RAISERROR('CREATE DATABASE GenericProfiles;', 1, 1) WITH NOWAIT;

CREATE DATABASE GenericProfiles;