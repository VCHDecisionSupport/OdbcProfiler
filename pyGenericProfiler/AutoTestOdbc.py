import pyodbc
import uuid
import datetime
from collections import *
import json
import time
from AutoTestObjectRelationalMapper import *

dw_denodo_dsn = 'DSN_Denodo'
dw_sql_server_dsn = 'DSN_SqlServer'

"""takes pyodbc.columns (ie. ODBC meta function SQLColumns) record and returns dict of meta data"""
def columns_parser(colums_meta_row):
    fmt_dict = {}
    fmt_dict['database_name'] = colums_meta_row[0]
    fmt_dict['schema_name'] = colums_meta_row[1]
    fmt_dict['table_name'] = colums_meta_row[2]
    fmt_dict['column_name'] = colums_meta_row[3]
    
    row_dict = {}
    row_dict['column_name'] = colums_meta_row[3]
    row_dict['ansi_full_column_name'] = '"{schema_name}"."{table_name}"."{column_name}"'.format(**fmt_dict)
    row_dict['ansi_full_table_name'] = '"{schema_name}"."{table_name}"'.format(**fmt_dict)
    return row_dict

"""takes pyodbc.tables (ie. ODBC meta function SQLTables) record and returns dict of meta data"""
def tables_parser(tables_meta_row):
    row_dict = {}
    # row_dict['database_name'] = tables_meta_row[0]
    row_dict['schema_name'] = tables_meta_row[1]
    row_dict['table_name'] = tables_meta_row[2]
    # row_dict['table_type'] = tables_meta_row[3]
    row_dict['ansi_full_table_name'] = '"{schema_name}"."{table_name}"'.format(**row_dict)
    return row_dict

class AutoTestOdbc(object):
    def __init__(self, connection_lambda, server_name, database_name, server_type, ansi_column_table_format, ansi_column_format):
        self.connection_lambda = connection_lambda
        self.server_name = server_name
        self.database_name = database_name
        self.server_type = server_type
        self.ansi_column_format = ansi_column_format
        self.ansi_column_table_format = ansi_column_table_format
        self.server_odbc_connection_string = self.connection_lambda(self.server_name, self.database_name)
        self.connection = None
        self.connect()

        self.profile_saver = AutoTestOrm()

        self.server_info = {}
        self.server_info['server_name'] = self.server_name
        self.server_info['server_type'] = self.server_type
        self.server_info_id = self.profile_saver.log_server_info(**self.server_info)
        self.server_info['server_info_id'] = self.server_info_id

        self.database_info = {}
        self.database_info['database_name'] = self.database_name
        self.database_info['server_info_id'] = self.server_info_id
        self.database_info_id = self.profile_saver.log_database_info(**self.database_info)
        self.database_info['database_info_id'] = self.database_info_id
        self.table_profiles = []
        self.column_profiles = []
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

    
    def process_meta_data(self):
        for table_meta_row in self.tables():
            print(tables_parser(table_meta_row))
            x = tables_parser(table_meta_row)['ansi_full_table_name']
            print(x)
        self.table_infos = {(tables_parser(table_meta_row)['ansi_full_table_name'],tables_parser(table_meta_row)) for table_meta_row in self.tables()}
        self.table_infos = [tables_parser(table_meta_row) for table_meta_row in self.tables()]
        self.column_infos = [columns_parser(column_meta_row) for column_meta_row in self.columns()]
        self.table_info_id_dict = {}
        for table_meta_dict in self.table_infos:
            table_meta_dict['database_info_id'] = self.database_info_id
            table_info_id = self.profile_saver.log_table_info(**table_meta_dict)
            table_meta_dict['table_info_id'] = table_info_id
        for column_meta_dict in self.column_infos:
            table_info_id = table_meta_dict[column_meta_dict['ansi_full_table_name']]['table_info_id']
            column_meta_dict['table_info_id'] = table_info_id
            column_info_id = self.profile_saver.log_column_info(**column_meta_dict)
            column_meta_dict['column_info_id'] = column_info_id
    
    def profile_database(self):
        profile_datetime = datetime.datetime.today()
        
        for table_info_dict in self.table_infos:
            table_profile = {}

            table_profile['table_info_id'] = table_info_dict['table_info_id']
            table_profile_sql = 'SELECT COUNT(*) AS "table_row_count" FROM {ansi_full_table_name};'.format(**table_info_dict)
            
            start_time = time.perf_counter()
            odbc_cur = self.connection.cursor()
            odbc_cur.execute(table_profile_sql)
            table_row_count = odbc_cur.fetchone()[0]
            odbc_cur.close()
            table_row_count_execution_seconds = end_time - time.perf_counter()
            table_profile['table_row_count'] = table_row_count
            table_profile['table_row_count_execution_seconds'] = table_row_count_execution_seconds
            table_row_count_datetime = profile_datetime
            table_profile['table_row_count_datetime'] = table_row_count_datetime
            table_profile_id = self.profile_saver.log_table_profile(**table_profile)
            table_profile['table_profile_id'] = table_profile_id
            self.table_profiles[table_profile['ansi_full_table_name']] = table_profile

        if self.profile_columns:
            for column_info_dict in self.column_infos:   
                column_profile = {}
                column_profile['table_profile_id'] = self.table_profiles['ansi_full_table_name']['table_profile_id']

                column_profile['column_info_id'] = column_info_dict['column_info_id']
                column_profile_sql = 'SELECT COUNT(DISTINCT {ansi_full_column_name}) AS "column_distinct_row_count" FROM {ansi_full_table_name};'.format(**column_info_dict)
                
                start_time = time.perf_counter()
                odbc_cur = self.connection.cursor()
                odbc_cur.execute(column_profile_sql)
                column_row_count = odbc_cur.fetchone()[0]
                odbc_cur.close()
                column_row_count_execution_seconds = end_time - time.perf_counter()
                column_profile['column_row_count'] = column_row_count
                column_profile['column_row_count_execution_seconds'] = column_row_count_execution_seconds
                column_row_count_datetime = profile_datetime
                column_profile['column_row_count_datetime'] = column_row_count_datetime
                column_profile_id = self.profile_saver.log_column_profile(**column_profile)
                column_profile['column_profile_id'] = column_profile_id
                self.column_profiles[column_profile['ansi_full_column_name']] = column_profile

                if column_row_count <= self.histogram_cutoff:

                    column_histogram_sql = 'SELECT COUNT(*) AS "column_value_count", {ansi_full_column_name} AS "column_value" FROM {ansi_full_table_name} GROUP BY {ansi_full_table_name}'.format(**column_info_dict)

                    start_time = time.perf_counter()
                    odbc_cur = self.connection.cursor()
                    odbc_cur.execute(column_histogram_sql)
                    column_histogram = list(odbc_cur.fetchall())
                    odbc_cur.close()
                    column_histogram_execution_seconds = end_time - time.perf_counter()
                    column_histogram_pairs = []
                    for column_histogram_pair in column_histogram:
                        column_histogram_pair_dict = {}
                        column_histogram_pair_dict['column_profile_id'] = column_profile_id
                        column_histogram_pair_dict['column_info_id'] = column_info_dict['column_info_id']
                        column_histogram_pair_dict['column_value_count'] = column_histogram_pair[0]
                        column_histogram_pair_dict['column_value_string'] = column_histogram_pair[1]
                        column_histogram_pairs.append(column_histogram_pair_dict)
                    column_profile_id = self.profile_saver.log_column_histogram(column_histogram_pairs)
class DenodoProfiler(AutoTestOdbc):
    def __init__(self, server_name, database_name):
        connection_lambda = lambda server_name, database_name, port=9999: "DSN={}".format(dw_denodo_dsn)
        server_type = 'Denodo'
        ansi_column_format = '"{table_owner}"."{table_name}"'
        ansi_column_table_format = '"{table_owner}"."{table_name}"."{column_name}"'
        super(DenodoProfiler, self).__init__(connection_lambda, server_name, database_name, server_type = server_type, ansi_column_table_format = ansi_column_table_format, ansi_column_format = ansi_column_format)


class SqlServerProfiler(AutoTestOdbc):
    def __init__(self, server_name, database_name):
        connection_lambda = lambda server_name, database_name: "DRIVER={ODBC Driver 13 for SQL Server};" + "SERVER={};DATABASE={};Trusted_Connection=Yes;".format(server_name, database_name)
        connection_lambda = lambda server_name, database_name, port=9999: "DSN={}".format(dw_sql_server_dsn)

        server_type = 'Sql Server'
        ansi_column_format = '"{table_schem}"."{table_name}"'
        ansi_column_table_format = '"{table_schem}"."{table_name}"."{column_name}"'
        # see https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolumns-function#comments
        # super(SqlServerProfiler, self).__init__(connection_lambda, server_name, database_name, server_type = 'Sql Server', odbc_tables_2_selectable_name = odbc_tables_2_sql_server_selectable_name)
        super(SqlServerProfiler, self).__init__(connection_lambda, server_name, database_name, server_type = server_type, ansi_column_table_format = ansi_column_table_format, ansi_column_format = ansi_column_format)
