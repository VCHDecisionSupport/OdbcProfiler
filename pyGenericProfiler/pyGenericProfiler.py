import pyodbc
import uuid
import GenericProfilesOrm
import datetime
from collections import *
import json

denodo_dsn = 'PC_Denodo'

denodo_con_lambda = lambda server_name, database_name, port=9996: "DRIVER={DenodoODBC Unicode(x64)};" + "SERVER={};DATABASE={};UID=gcrowell;PWD=gcrowell;PORT={};".format(server_name, database_name, port)
denodo_con_lambda = lambda server_name, database_name, port=9999: "DRIVER={DenodoODBC Unicode(x64)};" + "SERVER={};DATABASE={};UID=admin;PWD=admin;PORT={};".format(server_name, database_name, port)
denodo_con_lambda = lambda server_name, database_name, port=9999: "DSN={}".format(denodo_dsn)

sql_server_con_lambda = lambda server_name, database_name: "DRIVER={ODBC Driver 11 for SQL Server};" + "SERVER={};DATABASE={};Trusted_Connection=Yes;".format(server_name, database_name)


odbc_denodo_ansi_table_format = '"{table_cat}"."{table_name}"'
# see https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolumns-function#comments
odbc_denodo_ansi_column_format = '"{table_cat}"."{table_name}"."{column_name}"'
odbc_denodo_ansi_column_format = '"{table_name}"."{column_name}"'

def odbc_tables_2_sql_server_selectable_name(odbc_tables_meta_row):
    """converts results from a standard ODBC `SQLTables` meta data query to Denodo friendly (ie. selectable) object names"""
    return '"{}"."{}"."{}"'.format(odbc_tables_meta_row[0], odbc_tables_meta_row[1], odbc_tables_meta_row[2])

"""takes pyodbc Cursor object returns list of columns names of cursor's result"""
cursor_column_names = lambda cur: map(lambda cur_desc: cur_desc[0], cur.description)
"""takes pyodbc.tables (ie. SQLTables) record and returns table_name"""
table_meta_key = lambda tables_record: tables_record[2]
"""takes pyodbc.columns (ie. SQLColumns) record and returns column_name"""
column_meta_key = lambda columns_record: columns_record[3]

class OdbcConnection(object):
    def __init__(self, connection_lambda, server_name, database_name, server_type, ansi_table_format, ansi_column_format):
        self.server_name = server_name
        self.database_name = database_name
        self.connection_lambda = connection_lambda
        self.odbc_con_str = self.connection_lambda(self.server_name, self.database_name)
        self.server_type = server_type
        self.sql_alchemy_session = None
        self.connection = None
        self.table_meta_dict = None
        self.profile_columns = True
        self.ansi_table_format = ansi_table_format
        self.ansi_column_format = ansi_column_format
        self.full_meta_dict = None
        self.full_profile_dict = {server_name:{}}
        self.full_profile_dict[self.server_name]['server_type'] = self.server_type
        self.full_profile_dict[self.server_name]['tables'] = {}

    def connect(self):
        if self.connection is None:
            print('\tconnecting to: {}'.format(self.odbc_con_str))
            try:
                self.connection = pyodbc.connect(self.odbc_con_str)
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

    def create_tables_meta_dict(self):
        """returns dict(table_names: dict(table_meta_field_name, table_meta_field_value))"""
        """takes pyodbc Cursor object returns list of column names of cursor's result"""
        if self.table_meta_dict is None:
            temp_tables_cur = self.tables()
            cursor_column_names = lambda cur: map(lambda cur_desc: cur_desc[0], cur.description)
            table_meta_fields = list(cursor_column_names(temp_tables_cur))
            self.table_meta_dict = OrderedDict([(table_meta_key(table_meta), OrderedDict([(meta_meta_desc, meta_data) for meta_meta_desc, meta_data in zip(table_meta_fields,table_meta)])) for table_meta in temp_tables_cur])
            for key, meta_dict in self.table_meta_dict.items():
                self.table_meta_dict[key]['ansi_view_table_name'] = self.ansi_table_format.format(**self.table_meta_dict[key])
                self.table_meta_dict[key]['view_table_row_count_sql'] = 'SELECT COUNT(*) AS "view_table_row_count" FROM {};'.format(self.table_meta_dict[key]['ansi_view_table_name'])

    # def log_tables_info(self):
    #     """populate table_info table"""
    #     self.connect()
    #     self.create_tables_meta_dict()
    #     for key, meta_dict in self.table_meta_dict.items():
    #         view_table_info_id = self.sql_alchemy_session.log_viewtable_info(database_info_id=self.database_info_id, ansi_view_table_name=self.table_meta_dict[key]['ansi_view_table_name'], pretty_view_table_name=key)
    #         self.table_meta_dict[key]['view_table_info_id'] = view_table_info_id
    
    def columns(self,**kwargs):
        """returns pyodbc.cursor of all columns in current database
        (see https://github.com/mkleehammer/pyodbc/wiki/Cursor#columnstablenone-catalognone-schemanone-columnnone)
        """
        self.connect()
        cur = self.connection.cursor()
        return cur.columns(**kwargs)

    def create_columns_meta_dict(self):
        """column version of create_tables_meta_dict()"""
        """forwards kwargs param to self.tables(); returns dict(table_names: dict(table_meta_field_name, table_meta_field_value))"""
        """takes pyodbc Cursor object returns list of column names of cursor's result"""
        cursor_column_names = lambda cur: map(lambda cur_desc: cur_desc[0], cur.description)
        temp_columns_cur = self.columns()
        column_meta_fields = list(cursor_column_names(temp_columns_cur))
        
        self.colum_meta_dict = OrderedDict([(column_meta_key(column_meta), OrderedDict([(meta_meta_desc, meta_data) for meta_meta_desc, meta_data in zip(column_meta_fields,column_meta)])) for column_meta in temp_columns_cur])
        for key, meta_dict in self.colum_meta_dict.items():
            self.colum_meta_dict[key]['ansi_column_name'] = self.ansi_column_format.format(**self.colum_meta_dict[key])

    def get_full_meta_dict(self):
        """adds 'columns' key to table_meta_dict with column_meta_dict as it's value; 
        create dict of dict; combines self.table_meta_dict and self.column_meta_dict
        """
        if self.full_meta_dict is None:
            self.connect()
            self.create_columns_meta_dict()
            self.create_tables_meta_dict()
            col = self.colum_meta_dict
            temp_full_meta_dict = self.table_meta_dict
            # loop over each column in column_meta_dict and add it to full_meta_dict
            for column_name, column_meta in col.items():
                # add 'columns' as new items into table_meta_dict beside other table level meta data items
                if 'columns' not in temp_full_meta_dict[column_meta['table_name']]:
                    temp_full_meta_dict[column_meta['table_name']]['columns'] = {}
                # create sql code for column distinct count; and add sql code to column_meta_dict (which is then added to full_meta_dict)
                ansi_view_table_name = temp_full_meta_dict[column_meta['table_name']]['ansi_view_table_name']
                ansi_column_name = column_meta['ansi_column_name']
                column_distinct_count_sql = 'SELECT COUNT(DISTINCT {}) AS "column_distinct_count" FROM {};'.format(ansi_column_name, ansi_view_table_name)
                column_meta['column_distinct_count_sql'] = column_distinct_count_sql
                # create sql code for column value histogram
                column_histogram_sql = 'SELECT COUNT(*) AS "column_value_count", {ansi_column_name} AS "column_value_string" FROM {ansi_view_table_name} GROUP BY {ansi_column_name};'.format(ansi_column_name=ansi_column_name, ansi_view_table_name=ansi_view_table_name)
                column_meta['column_histogram_sql'] = column_histogram_sql
                # add column meta data
                temp_full_meta_dict[column_meta['table_name']]['columns'][column_name] = column_meta
            self.full_meta_dict = temp_full_meta_dict
            self.full_profile_dict[self.server_name]['tables'] = temp_full_meta_dict
        return self.full_meta_dict
    
    def execute_profile(self):
        temp_get_full_meta_dict = self.get_full_meta_dict()
        for table_name, table_meta in temp_get_full_meta_dict.items():
            print('execute sql: {}'.format(table_meta['view_table_row_count_sql']))
            view_table_row_count_cur = self.connection.cursor()
            view_table_row_count_cur.execute(table_meta['view_table_row_count_sql'])
            view_table_row_count = view_table_row_count_cur.fetchone()[0]
            temp_get_full_meta_dict[table_name]['view_table_row_count'] = view_table_row_count
            print(temp_get_full_meta_dict[table_name]['view_table_row_count'])
            
            for column_name, column_meta in temp_get_full_meta_dict[table_name]['columns'].items():
                print(column_name)
                column_distinct_count_sql = temp_get_full_meta_dict[table_name]['columns'][column_name]['column_distinct_count_sql']
                print('\texecute sql: {}'.format(temp_get_full_meta_dict[table_name]['columns'][column_name]['column_distinct_count_sql']))
                try:
                    column_distinct_count_cur = self.connection.cursor()
                    column_distinct_count_cur.execute(column_distinct_count_sql)
                    column_distinct_count = column_distinct_count_cur.fetchone()[0]
                    temp_get_full_meta_dict[table_name]['columns'][column_name]['column_distinct_count'] = column_distinct_count
                    print(temp_get_full_meta_dict[table_name]['columns'][column_name]['column_distinct_count'])
                except Exception as e:
                    print('ERROR: removing column: {} from profile'.format(column_name))
                    print(e)
                    del temp_get_full_meta_dict[table_name]['columns'][column_name]
            ## execute column histograms for columns that didn't error
            for column_name, column_meta in temp_get_full_meta_dict[table_name]['columns'].items():
                column_histogram_sql = temp_get_full_meta_dict[table_name]['columns'][column_name]['column_histogram_sql']
                print('\t\texecute sql: {}'.format(column_histogram_sql))
                try:
                    column_histogram_cur = self.connection.cursor()
                    column_histogram_cur.execute(column_histogram_sql)
                    column_histogram_dict = {}
                    # temp_get_full_meta_dict[table_name]['columns'][column_name]['column_histogram'] = {}
                    for column_histogram_record in column_histogram_cur.fetchall():
                        column_histogram_dict[str(column_histogram_record[1])] = column_histogram_record[0]
                    print_print(column_histogram_dict)
                    temp_get_full_meta_dict[table_name]['columns'][column_name]['column_histogram'] = column_histogram_dict
                except Exception as e:
                    print(e)
        self.full_meta_dict = temp_get_full_meta_dict
        self.full_profile_dict[self.server_name]['tables'] = temp_get_full_meta_dict
    def databases(self):
        raise NotImplemented("ERROR abstract method OdbcConnection.databases() not Implemented: retrieving databases requires a platform depandant implementation")

    def switch_database(self, database_name):
        return self.__class__(self.connection_lambda, self.server_name, database_name)

    # def profile_database(self):
    #     self.connect()
    #     self.log_tables_info()
    #     cur = self.connection.cursor()
    #     for name, meta in self.table_meta_dict.items():
    #         table_profile_sql = "SELECT COUNT(*) FROM {};".format(meta['ansi_view_table_name'])
    #         # print('{}'.format(selectable_name))
    #         row_count = cur.execute(table_profile_sql).fetchone()[0]
    #         # print('\t\t{}: row_count: {}'.format(selectable_name, row_count))
    #         view_table_profile_id = self.sql_alchemy_session.log_viewtable_profile(view_table_info_id=meta['view_table_info_id'], view_table_row_count=row_count, profile_date=datetime.datetime.now())
    #         self.table_meta_dict[name]['view_table_profile_id'] = view_table_profile_id
    #         if self.profile_columns:
    #             pass
            #     for column_meta in self.columns(table=selectable_name):
            #         column_select = self.odbc_columns_2_selectable_name(column_meta)
            #         column_profile_sql = "SELECT DISTINCT {} FROM {};".format(column_select, selectable_name)
            #         column_distinct_count = cur.execute(column_profile_sql).fetchone()[0]
            #         self.sql_alchemy_session.log_viewtable_profile(ViewTableProfileID=view_table_profile_id, ColumnDistinctRowCount=column_distinct_count, ColumnName=column_select)
class DenodoProfiler(OdbcConnection):
    def __init__(self, server_name, database_name):
        connection_lambda = denodo_con_lambda
        server_type = 'Denodo'
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

def print_print(in_dict, tab_count=0):
    for outer_key, outer_value in in_dict.items():
        print('{}key: {}'.format('\t'*tab_count, outer_key))
        if isinstance(outer_value, dict):
            print_print(outer_value, tab_count+1)
        else:
            print('{}{}'.format('\t'*tab_count,outer_value))

if __name__ == '__main__':
    session = GenericProfilesOrm.GenericProfiles()
    denodo = DenodoProfiler('PC', 'wide_world_importers')
    data_model_meta_dict = denodo.get_full_meta_dict()
    # print_print(data_model_meta_dict)
    denodo.execute_profile()
    tmp = denodo.full_profile_dict
    json_out = json.dumps(tmp, separators=(',', ':'), sort_keys=True, indent=4)
    f = open('full_meta_dict.json','w')
    f.write(str(json_out))
    f.close()


# from PyGenericProfiler import *

# denodo = DenodoProfiler('PC', 'wide_world_importers')
# data_model_meta_dict = denodo.get_full_meta_dict()
# print_print(data_model_meta_dict)
# denodo.execute_profile()
# tmp = denodo.full_meta_dict

# json_out = json.dumps(tmp, separators=(',', ':'), sort_keys=True, indent=4)
# print(json_out)

# f = open('full_meta_dict.json','w')
# f.write(str(json_out))
# f.close()