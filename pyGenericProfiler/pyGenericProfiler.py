import pyodbc
import uuid
import GenericProfilesOrm

denodo_dsn = 'DenodoODBC'
sql_server_dsn = 'DevOdbcSqlServer'
logging_dsn = 'GenericProfiles'

denodo_con_lambda = lambda server_name, database_name, port=9999: "DRIVER={DenodoODBC Unicode(x64)};" + "SERVER={};DATABASE={};UID=admin;PWD=admin;PORT={};".format(server_name, database_name, port)
denodo_con_lambda = lambda server_name, database_name, port=9996: "DRIVER={DenodoODBC Unicode(x64)};" + "SERVER={};DATABASE={};UID=gcrowell;PWD=gcrowell;PORT={};".format(server_name, database_name, port)

sql_server_con_lambda = lambda server_name, database_name: "DRIVER={ODBC Driver 11 for SQL Server};" + "SERVER={};DATABASE={};Trusted_Connection=Yes;".format(server_name, database_name)

# denodo_row_count_lambda = lambda database_name, schema_name, table_name: "SELECT COUNT(*) FROM {table_cat}.{table_name}".format(table_cat=database_name, table_name=table_name)

# sql_server_row_count_lambda = lambda database_name, schema_name, table_name: "SELECT COUNT(*) FROM {table_cat}.{table_schem}.{table_name}".format(table_cat=database_name, table_schem=schema_name, table_name=table_name)

denodo_physical_view_table_name_lambda = lambda database_name, schema_name, table_name: '"{table_cat}"."{table_name}"'.format(table_cat=database_name, table_name=table_name)
sql_server_physical_view_table_name_lambda = lambda database_name, schema_name, table_name: '"{table_cat}"."{table_schem}"."{table_name}"'.format(table_cat=database_name, table_schem=schema_name, table_name=table_name)

class OdbcConnection(object):
    def __init__(self, connection_lambda, server_name, database_name):
        self.server_name = server_name
        self.database_name = database_name
        self.connection_lambda = connection_lambda
        self.odbc_con_str = self.connection_lambda(self.server_name, self.database_name)
        # # set by derived class
        # self.row_count_query_format = None
        # set by derived class
        self.physical_name_format = None
        # set by derived class
        self.server_type = None
        self.sql_alchemy_session = None
        self.connection = None

    def connect(self):
        if self.connection is None:
            print('\tconnecting to: {}'.format(self.odbc_con_str))
            try:
                self.connection = pyodbc.connect(self.odbc_con_str)
            except Exception as e:
                print(e)
                raise e
    def set_log_session(self, alchemy_session):
        self.sql_alchemy_session = alchemy_session
        self.server_id = self.sql_alchemy_session.log_server_info(ServerName=self.server_name, ServerType=self.server_type)
        self.database_id = self.sql_alchemy_session.log_database_info(ServerInfoID=self.server_id, DatabaseName=self.database_name)
        print(self.database_id)

    def __del__(self):
        # print('disconnecting dsn name: {}'.format(self.server_name))
        print(self)
        print(self.__dict__())
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

    def log_tables_info(self):
        """asdf"""
        # physical_names = map(        )
        self.connect()
        tab_fmt = lambda item: '"{}"."{}"'.format(item[0],item[2])
        tab_names = [tab_fmt(tab) for tab in self.tables()]
        for table in tab_names:
            self.sql_alchemy_session.log_viewtable_info(DatabaseInfoID=self.database_id, PhysicalViewTableName=table)

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
        row_count = cur.execute(sql).fetchone()


class DenodoProfiler(OdbcConnection):
    def __init__(self, server_name, database_name):
        connection_lambda = denodo_con_lambda
        self.server_type = 'Denodo'
        self.physical_name_lambda = denodo_physical_view_table_name_lambda
        super(DenodoProfiler, self).__init__(connection_lambda, server_name, database_name)

    def databases(self):
        databases_query = 'SELECT DISTINCT DATABASE_NAME FROM CATALOG_VDP_METADATA_VIEWS();'
        self.connect()
        cur = self.connection.cursor()
        cur.execute(databases_query)
        return cur

class SqlServerProfiler(OdbcConnection):
    def __init__(self, server_name, database_name):
        connection_lambda = sql_server_con_lambda
        self.server_type = 'Sql Server'
        self.physical_name_lambda = sql_server_physical_view_table_name_lambda
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
    session = GenericProfilesOrm.GenericProfiles()
    denodo = DenodoProfiler('SPAPPDEN001', 'sandbox_paris')
    denodo.set_log_session(session)
    denodo.log_tables_info()
    exit

    # dsdw = SqlServerProfiler(sql_server_con_lambda, 'STDBDECSUP02', 'DSDW')
    # tabs = denodo.tables()
    # for tab in tabs:
    #     print(tab)
    #     sql = "SELECT COUNT(*) FROM {table_cat}.{table_schem}.{table_name}".format(table_cat=tab.table_cat, table_schem=tab.table_schem, table_name=tab.table_name)
    #     cur = denodo.connection.cursor()
    #     row_count = cur.execute(sql).fetchone()
    #     print(row_count)



