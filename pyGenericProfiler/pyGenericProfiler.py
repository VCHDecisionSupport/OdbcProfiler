import pyodbc
import uuid


denodo_dsn = 'DenodoODBC'
sql_server_dsn = 'DevOdbcSqlServer'
logging_dsn = 'GenericProfiles'

denodo_con_lambda = lambda server_name, database_name, port=9996: "DRIVER={DenodoODBC Unicode(x64)};" + "SERVER={};DATABASE={};UID=gcrowell;PWD=gcrowell;PORT={};".format(server_name, database_name, port)

sql_server_con_lambda = lambda server_name, database_name: "DRIVER={ODBC Driver 11 for SQL Server};" + "SERVER={};DATABASE={};Trusted_Connection=Yes;".format(server_name, database_name)

class OdbcConnection(object):
    def __init__(self, connection_lambda, server_name, database_name):
        self.server_name = server_name
        self.database_name = database_name
        self.connection_lambda = connection_lambda
        self.odbc_con_str = self.connection_lambda(self.server_name, self.database_name)
        # self.row_count_query_format = row_count_query_format
        # self.connection = pyodbc.connect(self.odbc_con_str)
        self.connection = None

    def connect(self):
        if self.connection is None:
            print('\tconnecting to: {}'.format(self.odbc_con_str))
        try:
            self.connection = pyodbc.connect(self.odbc_con_str)
        except Exception as e:
            print(e)

    def __del__(self):
        # print('disconnecting dsn name: {}'.format(self.server_name))
        if self.connection is not None:
            print('committing changes to: {}'.format(
                self.server_name))
            self.connection.commit()
            print('\tclosing {}'.format(self.server_name))
            self.connection.close()

    def tables(self):
        """returns pyodbc.cursor of all tables"""
        self.connect()
        cur = self.connection.cursor()
        return cur.tables()

    def columns(self):
        """returns pyodbc.cursor of all columns"""
        self.connect()
        cur = self.connection.cursor()
        return cur.columns()

    def databases(self):
        raise NotImplemented("ERROR: retrieving databases requires a platform depandant implementation")

    def switch_database(self, database_name):
        return self.__class__(self.connection_lambda, self.server_name, database_name)

    def profile_database(self):
        self.connect()
        cur = self.connection.cursor()
        kwargs = {"database_name":self.database_name, "schema_name":"dfs"}
        # for table_name in self.tables():


class DenodoProfiler(OdbcConnection):
    def __init__(self, connection_lambda, server_name, database_name):
        super(DenodoProfiler, self).__init__(connection_lambda, server_name, database_name)

    def databases(self):
        databases_query = 'SELECT DISTINCT DATABASE_NAME FROM CATALOG_VDP_METADATA_VIEWS();'
        self.connect()
        cur = self.connection.cursor()
        cur.execute(databases_query)
        return cur

class SqlServerProfiler(OdbcConnection):
    def __init__(self, connection_lambda, server_name, database_name):
        super(SqlServerProfiler, self).__init__(connection_lambda, server_name, database_name)

    def databases(self):
        guid = uuid.uuid1()
        temp_table_name = 'db_names_'+str(guid)[:7]
        databases_query_setup = """
IF EXISTS(SELECT * FROM tempdb.sys.tables WHERE name ='{0}')
    DROP TABLE tempdb.dbo.{0};
CREATE TABLE tempdb.dbo.{0}(DatabaseName varchar(100));
INSERT INTO tempdb.dbo.{0}
EXEC sp_MSforeachdb 'SELECT ''?''';
"""
        databases_query = """
SELECT * FROM tempdb.dbo.{};"""
        databases_query_setup = databases_query_setup.format(temp_table_name)
        databases_query = databases_query.format(temp_table_name)
        self.connect()
        cur = self.connection.cursor()
        cur.execute(databases_query_setup)
        cur.execute(databases_query)
        return cur




if __name__ == '__main__':
    
    denodo = DenodoProfiler(denodo_con_lambda, 'SPAPPDEN001', 'sandbox_paris')
    tabs = denodo.tables()
    for tab in tabs:
        print(tab)
        sql = "SELECT COUNT(*) FROM {table_cat}.{table_name}".format(table_cat=tab.table_cat, table_name=tab.table_name)
        cur = denodo.connection.cursor()
        row_count = cur.execute(sql).fetchone()
        print(row_count)

    dsdw = SqlServerProfiler(sql_server_con_lambda, 'STDBDECSUP02', 'DSDW')
    tabs = denodo.tables()
    for tab in tabs:
        print(tab)
        sql = "SELECT COUNT(*) FROM {table_cat}.{table_schem}.{table_name}".format(table_cat=tab.table_cat, table_schem=tab.table_schem, table_name=tab.table_name)
        cur = denodo.connection.cursor()
        row_count = cur.execute(sql).fetchone()
        print(row_count)



