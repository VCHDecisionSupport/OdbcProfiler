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

odbc_denodo_ansi_table_format = '"{table_cat}"."{table_name}"'
# see https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolumns-function#comments
odbc_denodo_ansi_column_format = '"{table_cat}"."{table_name}"."{column_name}"'
odbc_denodo_ansi_column_format = '"{table_qualifier}"."{table_name}"."{column_name}"'

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
    def __init__(self, connection_lambda, server_name, database_name, server_type, ansi_table_format, ansi_column_format):
        self.server_name = server_name
        self.database_name = database_name
        self.connection_lambda = connection_lambda
        self.odbc_con_str = self.connection_lambda(self.server_name, self.database_name)
        # # set by derived class
        # self.row_count_query_format = None
        # set by derived class
        # self.odbc_tables_2_selectable_name = odbc_tables_2_selectable_name
        # self.odbc_columns_2_selectable_name = odbc_columns_2_selectable_name
        # set by derived class
        self.server_type = server_type
        self.sql_alchemy_session = None
        self.connection = None
        self.table_meta_dict = None
        self.profile_columns = True
        self.ansi_table_format = ansi_table_format
        self.ansi_column_format = ansi_column_format

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
        self.server_info_id = self.sql_alchemy_session.log_server_info(server_name=self.server_name, server_type=self.server_type)
        self.database_info_id = self.sql_alchemy_session.log_database_info(server_info_id=self.server_info_id, database_name=self.database_name)

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
        if self.table_meta_dict is None:
            temp_tables_cur = self.tables(**kwargs)
            cursor_column_names = lambda cur: map(lambda cur_desc: cur_desc[0], cur.description)
            table_meta_fields = list(cursor_column_names(temp_tables_cur))
            table_meta_key = lambda tables_record: tables_record[2]
            self.table_meta_dict = OrderedDict([(table_meta_key(table_meta), OrderedDict([(meta_meta_desc, meta_data) for meta_meta_desc, meta_data in zip(table_meta_fields,table_meta)])) for table_meta in temp_tables_cur])
            for key, meta_dict in self.table_meta_dict.items():
                self.table_meta_dict[key]['ansi_view_table_name'] = self.ansi_table_format.format(**self.table_meta_dict[key])
        return self.table_meta_dict


    def log_tables_info(self):
        """populate table_info table"""
        self.connect()
        self.tables_meta_dict()
        for key, meta_dict in self.table_meta_dict.items():
            view_table_info_id = self.sql_alchemy_session.log_viewtable_info(database_info_id=self.database_info_id, ansi_view_table_name=self.table_meta_dict[key]['ansi_view_table_name'], pretty_view_table_name=key)
            self.table_meta_dict[key]['view_table_info_id'] = view_table_info_id
    
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
        table_meta_key = lambda tables_record: tables_record[2]
        self.colum_meta_dict = OrderedDict([(table_meta_key(table_meta), OrderedDict([(meta_meta_desc, meta_data) for meta_meta_desc, meta_data in zip(table_meta_fields,table_meta)])) for table_meta in temp_tables_cur])
        for key, meta_dict in self.colum_meta_dict.items():
            print(self.colum_meta_dict[key])
            self.colum_meta_dict[key]['ansi_column_name'] = self.ansi_column_format.format(**self.colum_meta_dict[key])
        return self.colum_meta_dict


    def databases(self):
        raise NotImplemented("ERROR abstract method OdbcConnection.databases() not Implemented: retrieving databases requires a platform depandant implementation")


    def switch_database(self, database_name):
        return self.__class__(self.connection_lambda, self.server_name, database_name)

    def profile_database(self):
        self.connect()
        self.log_tables_info()
        cur = self.connection.cursor()
        for name, meta in self.table_meta_dict.items():
            table_profile_sql = "SELECT COUNT(*) FROM {};".format(meta['ansi_view_table_name'])
            # print('{}'.format(selectable_name))
            row_count = cur.execute(table_profile_sql).fetchone()[0]
            # print('\t\t{}: row_count: {}'.format(selectable_name, row_count))
            view_table_profile_id = self.sql_alchemy_session.log_viewtable_profile(view_table_info_id=meta['view_table_info_id'], view_table_row_count=row_count, profile_date=datetime.datetime.now())
            self.table_meta_dict[name]['view_table_profile_id'] = view_table_profile_id

            # if self.profile_columns:
            #     for column_meta in self.columns(table=selectable_name):
            #         column_select = self.odbc_columns_2_selectable_name(column_meta)
            #         column_profile_sql = "SELECT DISTINCT {} FROM {};".format(column_select, selectable_name)
            #         column_distinct_count = cur.execute(column_profile_sql).fetchone()[0]
            #         self.sql_alchemy_session.log_viewtable_profile(ViewTableProfileID=view_table_profile_id, ColumnDistinctRowCount=column_distinct_count, ColumnName=column_select)
class DenodoProfiler(OdbcConnection):
    def __init__(self, server_name, database_name):
        connection_lambda = denodo_con_lambda
        server_type = 'Denodo'
        odbc_tables_2_selectable_name = odbc_tables_2_denodo_selectable_name
        odbc_columns_2_selectable_name = odbc_columns_2_denodo_selectable_name
        ansi_table_format = odbc_denodo_ansi_table_format
        ansi_column_format = odbc_denodo_ansi_column_format
        super(DenodoProfiler, self).__init__(connection_lambda, server_name, database_name, server_type = server_type, ansi_table_format = ansi_table_format, ansi_column_format = ansi_column_format)

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
CREATE TABLE tempdb.dbo.{0}(database_name varchar(100));
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
    denodo = DenodoProfiler('PC', 'wide_world_importers')
    
    denodo.set_log_session(session)
    denodo.log_tables_info()
    denodo.profile_database()
    tab = denodo.table_meta_dict
    denodo.columns_meta_dict()
    col = denodo.colum_meta_dict
    print(col)
    for key, meta in col.items():
        if 'columns' not in tab[meta['table_name']]:
            tab[meta['table_name']]['columns'] = {}
        tab[meta['table_name']]['columns'][key] = meta

    
    for table_name, table_meta in tab.items():
        print('table_name = {}'.format(table_name))
        print('table_name = {}'.format(table_meta))
        for column_name, column_meta in table_meta['columns'].items():
            print('column_name = {}'.format(column_name))
        # exit

    # dsdw = SqlServerProfiler(sql_server_con_lambda, 'STDBDECSUP02', 'DSDW')
    # tabs = denodo.tables()
    # for tab in tabs:
    #     print(tab)
    #     sql = "SELECT COUNT(*) FROM {table_cat}.{table_schem}.{table_name}".format(table_cat=tab.table_cat, table_schem=tab.table_schem, table_name=tab.table_name)
    #     cur = denodo.connection.cursor()
    #     row_count = cur.execute(sql).fetchone()
    #     print(row_count)



