import pyodbc
import uuid
import datetime
from collections import *
import json
import time

denodo_dsn = 'DSN_Denodo'

denodo_con_lambda = lambda server_name, database_name, port=9996: "DRIVER={DenodoODBC Unicode(x64)};" + "SERVER={};DATABASE={};UID=gcrowell;PWD=gcrowell;PORT={};".format(server_name, database_name, port)
denodo_con_lambda = lambda server_name, database_name, port=9999: "DRIVER={DenodoODBC Unicode(x64)};" + "SERVER={};DATABASE={};UID=admin;PWD=admin;PORT={};".format(server_name, database_name, port)
denodo_con_lambda = lambda server_name, database_name, port=9999: "DSN={}".format(denodo_dsn)

sql_server_con_lambda = lambda server_name, database_name: "DRIVER={ODBC Driver 11 for SQL Server};" + "SERVER={};DATABASE={};Trusted_Connection=Yes;".format(server_name, database_name)


# see https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolumns-function#comments
# odbc_sql_server_ansi_table_format = '"{table_schem}"."{table_name}"'
odbc_sql_server_ansi_column_table_format = '"{table_schem}"."{table_name}"'
odbc_sql_server_ansi_column_format = '"{table_schem}"."{table_name}"."{column_name}"'

# odbc_denodo_ansi_table_format = '"{table_schem}"."{table_name}"'
odbc_denodo_ansi_column_table_format = '"{table_owner}"."{table_name}"'
odbc_denodo_ansi_column_format = '"{table_owner}"."{table_name}"."{column_name}"'


"""takes pyodbc Cursor object returns list of columns names of cursor's result"""
cursor_column_names = lambda cur: map(lambda cur_desc: cur_desc[0], cur.description)
"""takes pyodbc.tables (ie. SQLTables) record and returns table_name"""
table_meta_key = lambda tables_record: '"'+tables_record[1]+'"."'+tables_record[2]+'"'
"""takes pyodbc.columns (ie. SQLColumns) record and returns column_name"""
column_meta_key = lambda columns_record: '"'+columns_record[1]+'"."'+columns_record[2]+'"."'+columns_record[3]+'"'

def filter_out_view_tables(schema_view_table_name):
    if '"sys".' in schema_view_table_name or '"INFORMATION_SCHEMA".' in schema_view_table_name:
        return False
    else:
        return True

class OdbcConnection(object):
    def __init__(self, connection_lambda, server_name, database_name, server_type, ansi_column_table_format, ansi_column_format):
        self.server_name = server_name
        self.database_name = database_name
        self.connection_lambda = connection_lambda
        self.server_odbc_connection_string = self.connection_lambda(self.server_name, self.database_name)
        self.server_type = server_type
        self.sql_alchemy_session = None
        self.connection = None
        self.table_meta_dict = None
        self.ansi_column_format = ansi_column_format
        self.ansi_column_table_format = ansi_column_table_format
        self.profile_dict = None
        self.full_meta = {}
        self.full_meta['server_info'] = {}
        self.full_meta['server_info']['server_name'] = self.server_name
        self.full_meta['server_info']['server_type'] = self.server_type
        self.full_meta['server_info']['database_info'] = {}
        self.full_meta['server_info']['database_info']['database_name'] = self.database_name
        self.full_meta['server_info']['database_info']['view_tables'] = {}
        self.execute_column_profiles = False
        self.execute_column_histograms = False
        
    def connect(self):
        if self.connection is None:
            print('\tconnecting to: {}'.format(self.server_odbc_connection_string))
            try:
                self.connection = pyodbc.connect(self.server_odbc_connection_string)
                self.odbc_version = self.connection.getinfo(pyodbc.SQL_DRIVER_ODBC_VER)
                print('\t\tconnected (ODBC version: {})'.format(self.odbc_version))
            except Exception as e:
                print(e)
                raise e

    # def set_log_session(self, alchemy_session):
        # self.sql_alchemy_session = alchemy_session
        # self.server_info_id = self.sql_alchemy_session.log_server_info(server_name=self.server_name, server_type=self.server_type)
        # self.database_info_id = self.sql_alchemy_session.log_database_info(server_info_id=self.server_info_id, database_name=self.database_name)

    def __del__(self):
        # print('disconnecting dsn name: {}'.format(self.server_name))
        print(self)
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

    def columns(self,**kwargs):
        """returns pyodbc.cursor of all columns in current database
        (see https://github.com/mkleehammer/pyodbc/wiki/Cursor#columnstablenone-catalognone-schemanone-columnnone)
        """
        self.connect()
        cur = self.connection.cursor()
        return cur.columns(**kwargs)

    def create_meta_dicts(self):
        print('create_columns_meta_dict')
        """column version of create_tables_meta_dict()"""
        """forwards kwargs param to self.tables(); returns dict(table_names: dict(table_meta_field_name, table_meta_field_value))"""
        """takes pyodbc Cursor object returns list of column names of cursor's result"""
        cursor_column_names = lambda cur: map(lambda cur_desc: cur_desc[0], cur.description)
        temp_columns_cur = self.columns()
        column_meta_fields = list(cursor_column_names(temp_columns_cur))
        
        self.column_meta_dict = OrderedDict([(column_meta_key(column_meta), OrderedDict([(meta_meta_desc, meta_data) for meta_meta_desc, meta_data in zip(column_meta_fields,column_meta)])) for column_meta in temp_columns_cur])
        for schema_view_table_name, meta_dict in self.column_meta_dict.items():
            self.column_meta_dict[schema_view_table_name]['ansi_column_name'] = self.ansi_column_format.format(**self.column_meta_dict[schema_view_table_name])
            self.column_meta_dict[schema_view_table_name]['schema_view_table_name'] = self.ansi_column_table_format.format(**self.column_meta_dict[schema_view_table_name])
            self.column_meta_dict[schema_view_table_name]['column_distinct_count_sql'] = 'SELECT COUNT(DISTINCT {ansi_column_name}) AS "column_distinct_count" FROM {schema_view_table_name};'.format(**self.column_meta_dict[schema_view_table_name])
            self.column_meta_dict[schema_view_table_name]['column_histogram_sql'] = 'SELECT COUNT(*) AS "column_value_count", {ansi_column_name} AS "column_value_string" FROM {schema_view_table_name} GROUP BY {ansi_column_name};'.format(**self.column_meta_dict[schema_view_table_name])
        self.table_meta_dict = OrderedDict()
        for column_name, column in self.column_meta_dict.items():
            # print(column['schema_view_table_name'])
            if filter_out_view_tables(column['schema_view_table_name']):
                if column['schema_view_table_name'] not in self.table_meta_dict:
                    self.table_meta_dict[column['schema_view_table_name']] = {}
                    self.table_meta_dict[column['schema_view_table_name']]['schema_view_table_name'] = column['schema_view_table_name']
                    self.table_meta_dict[column['schema_view_table_name']]['view_table_row_count_sql'] = 'SELECT COUNT(*) AS "view_table_row_count" FROM {};'.format(column['schema_view_table_name'])
                    self.table_meta_dict[column['schema_view_table_name']]['columns'] = OrderedDict()
                self.table_meta_dict[column['schema_view_table_name']]['columns'][column['ansi_column_name']] = column
        self.full_meta['server_info']['database_info']['view_tables'] = self.table_meta_dict

    def execute_profile(self):
        print('execute_profile')
        self.create_meta_dicts()
        self.column_meta_dict
        temp_get_profile_dict = self.table_meta_dict

        for table_name, table_meta in self.table_meta_dict.items():
            print('execute sql: {}'.format(table_meta['view_table_row_count_sql']))
            try:
                view_table_row_count_cur = self.connection.cursor()
                start = time.perf_counter()
                view_table_row_count_cur.execute(table_meta['view_table_row_count_sql'])
                view_table_row_count = view_table_row_count_cur.fetchone()[0]
                view_table_row_count_cur.close()
                end = time.perf_counter()
                self.table_meta_dict[table_name]['view_table_row_count'] = view_table_row_count
                self.table_meta_dict[table_name]['view_table_row_count_execution_time'] = end-start
                print('view_table_row_count: {} ({} seconds)'.format(self.table_meta_dict[table_name]['view_table_row_count'], self.table_meta_dict[table_name]['view_table_row_count_execution_time']))
            except Exception as e:
                print(e)
            if self.execute_column_profiles:
                for column_name, column_meta in self.table_meta_dict[table_name]['columns'].items():
                    print(column_name)
                    column_distinct_count_sql = self.table_meta_dict[table_name]['columns'][column_name]['column_distinct_count_sql']
                    print('\texecute sql: {}'.format(self.table_meta_dict[table_name]['columns'][column_name]['column_distinct_count_sql']))
                    try:
                        column_distinct_count_cur = self.connection.cursor()
                        start = time.perf_counter()
                        column_distinct_count_cur.execute(column_distinct_count_sql)
                        column_distinct_count = column_distinct_count_cur.fetchone()[0]
                        column_distinct_count_cur.close()
                        end = time.perf_counter()
                        self.table_meta_dict[table_name]['columns'][column_name]['column_distinct_count_execution_time'] = end-start
                        self.table_meta_dict[table_name]['columns'][column_name]['column_distinct_count'] = column_distinct_count
                        print('column_distinct_count_execution_time: {} ({} seconds)'.format(self.table_meta_dict[table_name]['columns'][column_name]['column_distinct_count'],self.table_meta_dict[table_name]['columns'][column_name]['column_distinct_count_execution_time']))
                    except Exception as e:
                        print(e)
            if self.execute_column_histograms:
                ## execute column histograms for columns that didn't error
                for column_name, column_meta in self.table_meta_dict[table_name]['columns'].items():
                    column_histogram_sql = self.table_meta_dict[table_name]['columns'][column_name]['column_histogram_sql']
                    print('\t\texecute sql: {}'.format(column_histogram_sql))
                    if self.table_meta_dict[table_name]['columns'][column_name]['column_distinct_count'] <= 1000:
                        try:
                            column_histogram_cur = self.connection.cursor()
                            start = time.perf_counter()
                            column_histogram_cur.execute(column_histogram_sql)
                            column_histogram_dict = {}
                            # self.table_meta_dict[table_name]['columns'][column_name]['column_histogram'] = {}
                            for column_histogram_record in column_histogram_cur.fetchall():
                                column_histogram_dict[str(column_histogram_record[1])] = column_histogram_record[0]
                            column_histogram_cur.close()
                            end = time.perf_counter()
                            # print_print(column_histogram_dict)
                            self.table_meta_dict[table_name]['columns'][column_name]['column_histogram'] = column_histogram_dict
                            self.table_meta_dict[table_name]['columns'][column_name]['column_histogram_execution_time'] = end-start
                            print('column_histogram_execution_time: {} ({} seconds)'.format(self.table_meta_dict[table_name]['columns'][column_name]['column_histogram'],self.table_meta_dict[table_name]['columns'][column_name]['column_histogram_execution_time']))
                        except Exception as e:
                            print(e)
                    else:
                        print('column_distinct_count too large: {}'.format(self.table_meta_dict[table_name]['columns'][column_name]['column_distinct_count']))
        self.profile_dict = self.table_meta_dict
    
    def databases(self):
        raise NotImplemented("ERROR abstract method OdbcConnection.databases() not Implemented: retrieving databases requires a platform depandant implementation")

    def switch_database(self, database_name):
        return self.__class__(self.connection_lambda, self.server_name, database_name)

class DenodoProfiler(OdbcConnection):
    def __init__(self, server_name, database_name):
        connection_lambda = denodo_con_lambda
        server_type = 'Denodo'
        ansi_column_format = odbc_denodo_ansi_column_format
        ansi_column_table_format = odbc_denodo_ansi_column_table_format
        super(DenodoProfiler, self).__init__(connection_lambda, server_name, database_name, server_type = server_type, ansi_column_table_format = ansi_column_table_format, ansi_column_format = ansi_column_format)

    def databases(self):
        databases_query = 'SELECT DISTINCT DATABASE_NAME FROM CATALOG_VDP_METADATA_VIEWS();'
        self.connect()
        cur = self.connection.cursor()
        cur.execute(databases_query)
        return cur

class SqlServerProfiler(OdbcConnection):
    def __init__(self, server_name, database_name):
        connection_lambda = sql_server_con_lambda
        server_type = 'Sql Server'
        ansi_column_format = odbc_sql_server_ansi_column_format
        ansi_column_table_format = odbc_sql_server_ansi_column_table_format
        # super(SqlServerProfiler, self).__init__(connection_lambda, server_name, database_name, server_type = 'Sql Server', odbc_tables_2_selectable_name = odbc_tables_2_sql_server_selectable_name)
        super(SqlServerProfiler, self).__init__(connection_lambda, server_name, database_name, server_type = server_type, ansi_column_table_format = ansi_column_table_format, ansi_column_format = ansi_column_format)

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

def print_print(in_dict, tab_count=0):
    for outer_key, outer_value in in_dict.items():
        print('{}key: {}'.format('\t'*tab_count, outer_key))
        if isinstance(outer_value, dict):
            print_print(outer_value, tab_count+1)
        else:
            print('{}{}'.format('\t'*tab_count,outer_value))



# from pyGenericProfiler import *
# import pyGenericProfiler
# import json




# denodo = DenodoProfiler('PC', 'wide_world_importers')
# denodo.create_meta_dicts()
# columns_meta = denodo.column_meta_dict
# tables_meta = denodo.table_meta_dict
# denodo.execute_profile()

# print(list(column_meta.keys()))

# meta_dict = denodo.get_profile_dict()

# temp_get_profile_dict = denodo.profile_dict
# for table_name, table_meta in temp_get_profile_dict.items():
#     print('table_name: {}'.format(table_name))
#     print(list(temp_get_profile_dict[table_name].keys()))
# for table_name, table_meta in data_model_meta_dict.items():
#     print(table_name)
#     for column_name, column_meta in data_model_meta_dict[table_name]['columns'].items():
#         print(column_name)

# # print_print(data_model_meta_dict)
# denodo.execute_profile()
# # tmp = denodo.profile_dict
# json_out = json.dumps(tmp, separators=(',', ':'), sort_keys=True, indent=4)
# # print(json_out)

# f.write(str(json_out))
# f.close()

if __name__ == '__main__':
    session = GenericProfilesOrm.GenericProfiles()
    # denodo = DenodoProfiler('PC', 'wide_world_importers')
    # data_model_meta_dict = denodo.get_profile_dict()
    # print_print(data_model_meta_dict)
    # denodo.execute_profile()
    # json_out = json.dumps(tmp, separators=(',', ':'), sort_keys=True, indent=4)
    # f.write(str(json_out))
    # f.close()