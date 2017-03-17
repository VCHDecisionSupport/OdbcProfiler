import pyodbc
import uuid
import GenericProfilesOrm
import datetime
from collections import *


denodo_dsn = 'PC_Denodo'
sql_server_dsn = 'DevOdbcSqlServer'
logging_dsn = 'GenericProfiles'

denodo_con_lambda = lambda server_name, database_name, port=9996: "DRIVER={DenodoODBC Unicode(x64)};" + "SERVER={};DATABASE={};UID=gcrowell;PWD=gcrowell;PORT={};".format(server_name, database_name, port)
denodo_con_lambda = lambda server_name, database_name, port=9999: "DRIVER={DenodoODBC Unicode(x64)};" + "SERVER={};DATABASE={};UID=admin;PWD=admin;PORT={};".format(server_name, database_name, port)
denodo_con_lambda = lambda server_name, database_name, port=9999: "DSN={}".format(denodo_dsn)

sql_server_con_lambda = lambda server_name, database_name: "DRIVER={ODBC Driver 11 for SQL Server};" + "SERVER={};DATABASE={};Trusted_Connection=Yes;".format(server_name, database_name)

# denodo_row_count_lambda = lambda database_name, schema_name, table_name: "SELECT COUNT(*) FROM {table_cat}.{table_name}".format(table_cat=database_name, table_name=table_name)
# sql_server_row_count_lambda = lambda database_name, schema_name, table_name: "SELECT COUNT(*) FROM {table_cat}.{table_schem}.{table_name}".format(table_cat=database_name, table_schem=schema_name, table_name=table_name)
# denodo_physical_view_table_name_lambda = lambda database_name, schema_name, table_name: '"{table_cat}"."{table_name}"'.format(table_cat=database_name, table_name=table_name)
# sql_server_physical_view_table_name_lambda = lambda database_name, schema_name, table_name: '"{table_cat}"."{table_schem}"."{table_name}"'.format(table_cat=database_name, table_schem=schema_name, table_name=table_name)

def odbc_tables_2_denodo_selectable_name(odbc_tables_meta_row):
    """converts results from a standard ODBC `SQLTables` meta data query to Denodo friendly (ie. selectable) object names"""
    return '"{}"."{}"'.format(odbc_tables_meta_row[0], odbc_tables_meta_row[2])

def odbc_tables_2_sql_server_selectable_name(odbc_tables_meta_row):
    """converts results from a standard ODBC `SQLTables` meta data query to Denodo friendly (ie. selectable) object names"""
    return '"{}"."{}"."{}"'.format(odbc_tables_meta_row[0], odbc_tables_meta_row[1], odbc_tables_meta_row[2])

def odbc_columns_2_denodo_selectable_name(odbc_tables_meta_row):
    """converts results from a standard ODBC `SQLColumns` meta data query to Denodo friendly (ie. selectable) object names
    https://github.com/mkleehammer/pyodbc/wiki/Cursor#columnstablenone-catalognone-schemanone-columnnone"""
    return '"{}"."{}"."{}"'.format(odbc_tables_meta_row[0], odbc_tables_meta_row[2], odbc_tables_meta_row[3])

def odbc_columns_2_sql_server_selectable_name(odbc_tables_meta_row):
    """converts results from a standard ODBC `SQLColumns` meta data query to Denodo friendly (ie. selectable) object names
    https://github.com/mkleehammer/pyodbc/wiki/Cursor#columnstablenone-catalognone-schemanone-columnnone"""
    return '"{}"."{}"."{}"."{}"'.format(odbc_tables_meta_row[0], odbc_tables_meta_row[1], odbc_tables_meta_row[2], odbc_tables_meta_row[3])

"""takes pyodbc Cursor object returns list of column names of cursor's result"""
cursor_column_names = lambda cur: map(lambda cur_desc: cur_desc[0], cur.description)

class OdbcConnection(object):
    def __init__(self, connection_lambda, server_name, database_name, server_type, odbc_tables_2_selectable_name, odbc_columns_2_selectable_name):
        self.server_name = server_name
        self.database_name = database_name
        self.connection_lambda = connection_lambda
        self.odbc_con_str = self.connection_lambda(self.server_name, self.database_name)
        # # set by derived class
        # self.row_count_query_format = None
        # set by derived class
        self.odbc_tables_2_selectable_name = odbc_tables_2_selectable_name
        self.odbc_columns_2_selectable_name = odbc_columns_2_selectable_name
        # set by derived class
        self.server_type = server_type
        self.sql_alchemy_session = None
        self.connection = None
        self.table_name_id = {}
        self.profile_columns = True

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


    def tables(self, **kwargs):
        """returns pyodbc.cursor of all tables (see https://github.com/mkleehammer/pyodbc/wiki/Cursor#tablestablenone-catalognone-schemanone-tabletypenone)"""
        self.connect()
        cur = self.connection.cursor()
        return cur.tables(**kwargs)


    def tables_meta_dict(self, **kwargs):
        """forwards kwargs param to self.tables(); returns dict(table_names: dict(table_meta_field_name, table_meta_field_value))"""
        """takes pyodbc Cursor object returns list of column names of cursor's result"""
        cursor_column_names = lambda cur: map(lambda cur_desc: cur_desc[0], cur.description)
        temp_tables_cur = self.tables(**kwargs)
        table_meta_fields = list(cursor_column_names(temp_tables_cur))
        return OrderedDict([(table_meta_key(table_meta), OrderedDict([(meta_meta_desc, meta_data) for meta_meta_desc, meta_data in zip(table_meta_fields,table_meta)])) for table_meta in temp_tables_cur])


    def log_tables_info(self):
        """asdf"""
        # physical_names = map(        )
        self.connect()
        selectable_names = [self.odbc_tables_2_selectable_name(tab) for tab in self.tables()]
        for selectable_name in selectable_names:
            self.table_name_id[selectable_name] = self.sql_alchemy_session.log_viewtable_info(DatabaseInfoID=self.database_id, PhysicalViewTableName=selectable_name)

    def columns(self,**kwargs):
        """returns pyodbc.cursor of all columns (see https://github.com/mkleehammer/pyodbc/wiki/Cursor#columnstablenone-catalognone-schemanone-columnnone)"""
        self.connect()
        cur = self.connection.cursor()
        return cur.columns(**kwargs)
    
    def columns_meta_dict(self, **kwargs):
        """column version of tables_meta_dict()"""
        """forwards kwargs param to self.tables(); returns dict(table_names: dict(table_meta_field_name, table_meta_field_value))"""
        """takes pyodbc Cursor object returns list of column names of cursor's result"""
        cursor_column_names = lambda cur: map(lambda cur_desc: cur_desc[0], cur.description)
        temp_tables_cur = self.columns(**kwargs)
        table_meta_fields = list(cursor_column_names(temp_tables_cur))
        col_meta = OrderedDict([(table_meta_key(table_meta), OrderedDict([(meta_meta_desc, meta_data) for meta_meta_desc, meta_data in zip(table_meta_fields,table_meta)])) for table_meta in temp_tables_cur])

    def databases(self):
        raise NotImplemented("ERROR abstract method OdbcConnection.databases() not Implemented: retrieving databases requires a platform depandant implementation")

    def switch_database(self, database_name):
        return self.__class__(self.connection_lambda, self.server_name, database_name)

    def profile_database(self):
        self.connect()
        self.log_tables_info()
        cur = self.connection.cursor()
        for selectable_name, selectable_id in self.table_name_id.items():
            table_profile_sql = "SELECT COUNT(*) FROM {};".format(selectable_name)
            print('{}'.format(selectable_name))
            row_count = cur.execute(table_profile_sql).fetchone()[0]
            print('\t\t{}: row_count: {}'.format(selectable_name, row_count))
            view_table_profile_id = self.sql_alchemy_session.log_viewtable_profile(ViewTableInfoID=selectable_id, ViewTableRowCount=row_count, ProfileDate=datetime.datetime.now())
            if self.profile_columns:
                for column_meta in self.columns(table=selectable_name):
                    column_select = self.odbc_columns_2_selectable_name(column_meta)
                    column_profile_sql = "SELECT DISTINCT {} FROM {};".format(column_select, selectable_name)
                    column_distinct_count = cur.execute(column_profile_sql).fetchone()[0]
                    self.sql_alchemy_session.log_viewtable_profile(ViewTableProfileID=view_table_profile_id, ColumnDistinctRowCount=column_distinct_count, ColumnName=column_select)
class DenodoProfiler(OdbcConnection):
    def __init__(self, server_name, database_name):
        connection_lambda = denodo_con_lambda
        server_type = 'Denodo'
        odbc_tables_2_selectable_name = odbc_tables_2_denodo_selectable_name
        odbc_columns_2_selectable_name = odbc_columns_2_denodo_selectable_name
        super(DenodoProfiler, self).__init__(connection_lambda, server_name, database_name, server_type = server_type, odbc_tables_2_selectable_name = odbc_tables_2_denodo_selectable_name, odbc_columns_2_selectable_name = odbc_columns_2_selectable_name)

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
        self.odbc_tables_2_selectable_name = odbc_tables_2_sql_server_selectable_name
        super(SqlServerProfiler, self).__init__(connection_lambda, server_name, database_name, server_type = 'Sql Server', odbc_tables_2_selectable_name = odbc_tables_2_sql_server_selectable_name)

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

SqlColumnsKeyNames = ['table_cat', 'table_schem', 'table_name', 'column_name', 'data_type', 'type_name', 'column_size', 'buffer_length', 'decimal_digits', 'num_prec_radix', 'nullable', 'remarks', 'column_def', 'sql_data_type', 'sql_datetime_sub', 'char_octet_length', 'ordinal_position', 'is_nullable']
SqlTablesKeyNames = ['table_cat', 'table_schem', 'table_name', 'table_type', 'remarks']

if __name__ == '__main__':
    session = GenericProfilesOrm.GenericProfiles()
    denodo = DenodoProfiler('PC', 'wide_world_importers')
    tables = denodo.tables()

    table_meta_key = lambda table_meta: table_meta[2]
    table_meta_fields = list(cursor_column_names(tables))
    # tables_meta_dict = OrderedDict()
    # for table_meta in tables:
    #     table_name = table_meta_key(table_meta)
    #     table_meta_dict = OrderedDict([(table_meta_desc, table_meta_value) for table_meta_desc, table_meta_value in zip(table_meta_fields, table_meta)])
    #     tables_meta_dict[table_name] = table_meta_dict
    
    # x = OrderedDict([(table_meta_key(table_meta), OrderedDict([(meta_meta_desc, meta_data) for meta_meta_desc, meta_data in zip(table_meta_fields,table_meta)])) for table_meta in tables])
    # # print(x)
    # for name, meta in x.items():
    #     print('\nname: {}'.format(name))
    #     for meta_desc, meta_data in meta.items():
    #         print('\nmeta_field({}):meta_value({})'.format(meta_desc, meta_data))


    cursor_column_names = lambda cur: map(lambda cur_desc: cur_desc[0], cur.description)
    temp_tables_cur = denodo.columns()
    table_meta_fields = list(cursor_column_names(temp_tables_cur))
    col_meta = OrderedDict([(table_meta_key(table_meta), OrderedDict([(meta_meta_desc, meta_data) for meta_meta_desc, meta_data in zip(table_meta_fields,table_meta)])) for table_meta in temp_tables_cur])
    
    for name, meta in col_meta.items():
        print('name: {}'.format(name))
        for meta_desc, meta_data in meta.items():
            print('\tmeta_field({}):meta_value({})'.format(meta_desc, meta_data))

    # print(x['city'])
    # print(x['city']['table_type'])

    # for table_meta in tables:
    #     print(table_meta_key(table_meta))
    #     for meta_meta_desc, meta_data in zip(tables.description,table_meta):
    #         print('key({}):value({})'.format(meta_meta_desc[0], meta_data))
    
    # x = OrderedDict([(table_meta_key(table_meta), OrderedDict([(meta_meta_desc[0], meta_data) for meta_meta_desc, meta_data in zip(tables.description,table_meta)])) for table_meta in tables])
    # print(x)
    # print(x['city'])
    # print(x['city']['table_type'])


    # collections.OrderedDict([(column[3],{name[0]:value for name, value in zip(columns.description,column)} for column in self.columns()]
    # for column in columns:
    #     fulldict = {name[0]:value for name, value in zip(columns.description,column)}
    #     print(fulldict)
    # abracadabra=[{name[0]:value for name, value in zip(columns.description,column)} for column in columns]
    # print(abracadabra)
    # for column in abracadabra:
    #     print(column['column_name'])
    # print(columns.description)
    # for column in columns:
    # print(len(columns.description))
    # print(len(list(columns)[0]))
    # print(columns)
    # meta = {SqlColumnsKeyNames[i]:columns[i] for i in range(len(columns))}        
    # print(meta)
    # denodo.set_log_session(session)
    # denodo.log_tables_info()
    # denodo.profile_database()
    # exit

    # dsdw = SqlServerProfiler(sql_server_con_lambda, 'STDBDECSUP02', 'DSDW')
    # tabs = denodo.tables()
    # for tab in tabs:
    #     print(tab)
    #     sql = "SELECT COUNT(*) FROM {table_cat}.{table_schem}.{table_name}".format(table_cat=tab.table_cat, table_schem=tab.table_schem, table_name=tab.table_name)
    #     cur = denodo.connection.cursor()
    #     row_count = cur.execute(sql).fetchone()
    #     print(row_count)



