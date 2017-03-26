import pyodbc
import uuid
import datetime
from collections import *
import json
import time
from OdbcProfiler import AutoTestObjectRelationalMapper as orm

dw_denodo_dsn = 'DSN_Denodo'
dw_sql_server_dsn = 'DSN_SqlServer'

skip_schemas = 'sys', 'INFORMATION_SCHEMA'

"""takes pyodbc.columns (ie. ODBC meta function SQLColumns) record and returns dict of meta data"""
def columns_parser(columns_meta_row):
    fmt_dict = {}
    fmt_dict['database_name'] = columns_meta_row[0]
    fmt_dict['schema_name'] = columns_meta_row[1]
    fmt_dict['table_name'] = columns_meta_row[2]
    fmt_dict['column_name'] = columns_meta_row[3]
    row_dict = {}
    row_dict['column_name'] = columns_meta_row[3]
    row_dict['ansi_full_column_name'] = '"{schema_name}"."{table_name}"."{column_name}"'.format(**fmt_dict)
    row_dict['ansi_full_table_name'] = '"{schema_name}"."{table_name}"'.format(**fmt_dict)
    return row_dict

"""takes pyodbc.tables (ie. ODBC meta function SQLTables) record and returns dict of meta data"""
def tables_parser(tables_meta_row):
    row_dict = {}
    row_dict['schema_name'] = tables_meta_row[1]
    row_dict['table_name'] = tables_meta_row[2]
    row_dict['table_type'] = tables_meta_row[3]
    row_dict['ansi_full_table_name'] = '"{schema_name}"."{table_name}"'.format(**row_dict)
    return row_dict

class AutoTestOdbc(object):
    """base class ODBC data source to be profiled"""
    def __init__(self, connection_lambda, server_name, database_name, ansi_column_table_format, ansi_column_format):
        self.connection_lambda = connection_lambda
        self.server_name = server_name
        self.database_name = database_name
        self.ansi_column_format = ansi_column_format
        self.ansi_column_table_format = ansi_column_table_format
        self.server_odbc_connection_string = self.connection_lambda(self.server_name, self.database_name)
        self.connection = None
        self.server_type = None
        self.connect()

        self.profile_saver = orm.AutoTestOrm()

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

        self.profile_columns = True
        self.histogram_cutoff = 1000
        self.table_profiles = {}
        self.column_profiles = {}
    def connect(self):
        if self.connection is None:
            print('\tconnecting to ODBC: {}'.format(self.server_odbc_connection_string))
            try:
                self.connection = pyodbc.connect(self.server_odbc_connection_string)
                self.odbc_version = self.connection.getinfo(pyodbc.SQL_DRIVER_ODBC_VER)
                self.odbc_dbms_name = self.connection.getinfo(pyodbc.SQL_DBMS_NAME)
                self.odbc_database_name = self.connection.getinfo(pyodbc.SQL_DATABASE_NAME)
                self.odbc_server_name = self.connection.getinfo(pyodbc.SQL_SERVER_NAME)
                self.server_name = self.odbc_server_name
                self.database_name = self.odbc_database_name
                self.server_type = self.odbc_dbms_name
                print('\t\tconnected.\n\t\tODBC version: {}\n\t\tdbms_name: {}\n\t\todbc_server_name: {}\n\t\tdatabase_name {}'.format(self.odbc_version, self.odbc_dbms_name, self.odbc_server_name, self.odbc_database_name))
            except Exception as e:
                print(e)
                raise e

    def __del__(self):
        # print('disconnecting dsn name: {}'.format(self.server_name))
        # print(self)
        if self.connection is not None:
            print('committing changes to: {}'.format(
                self.server_name))
            self.connection.commit()
            print('\tclosing {}'.format(self.server_name))
            self.connection.close()

    def tables(self, **kwargs):
        """equivalent to SQLTables in ODBC C-API
        (see https://github.com/mkleehammer/pyodbc/wiki/Cursor#tablestablenone-catalognone-schemanone-tabletypenone)"""
        self.connect()
        cur = self.connection.cursor()
        return cur.tables(**kwargs)

    
    def columns(self,**kwargs):
        """equivalent to SQLColumns in ODBC C-API
        (see https://github.com/mkleehammer/pyodbc/wiki/Cursor#columnstablenone-catalognone-schemanone-columnnone)
        """
        self.connect()
        cur = self.connection.cursor()
        return cur.columns(**kwargs)

    
    def process_meta_data(self):
        """update meta data logs"""
        self.table_infos = {}
        print('\nprocess tables()')
        valid_schemas = set()
        for table_meta_row in self.tables():
            table_info = tables_parser(table_meta_row)
            """skip sys and INFORMATION_SCHEMA"""
            if table_info['schema_name'] not in skip_schemas:
                ansi_full_table_name = table_info['ansi_full_table_name']
                self.table_infos[ansi_full_table_name] = tables_parser(table_meta_row)
                valid_schemas.add(table_info['schema_name'])
        print('process columns()')
        self.column_infos = {}
        """get columns in tables with non-skipped valid schemas"""
        for schema_name in valid_schemas:
            for column_meta_row in self.columns(schema=schema_name):
                ansi_full_column_name = columns_parser(column_meta_row)['ansi_full_column_name']
                self.column_infos[ansi_full_column_name] = columns_parser(column_meta_row)
        print('log table_info')
        self.table_info_id_dict = {}
        for table_meta_dict in self.table_infos.values():
            table_meta_dict['database_info_id'] = self.database_info_id
            """save to logging database with sqlalchemy"""
            table_info_id = self.profile_saver.log_table_info(**table_meta_dict)
            table_meta_dict['table_info_id'] = table_info_id
            self.table_info_id_dict[table_meta_dict['ansi_full_table_name']] = table_info_id
        print('log column_info')
        for column_meta_dict in self.column_infos.values():
            if column_meta_dict['ansi_full_table_name'] in self.table_info_id_dict:
                table_info_id = self.table_info_id_dict[column_meta_dict['ansi_full_table_name']]
            column_meta_dict['table_info_id'] = table_info_id
            """save to logging database with sqlalchemy"""
            column_info_id = self.profile_saver.log_column_info(**column_meta_dict)
            column_meta_dict['column_info_id'] = column_info_id
        print('table count: {} column count: {}'.format(len(self.table_infos), len(self.column_infos)))
    def profile_database(self):
        """execute and log profiling queries"""
        profile_datetime = datetime.datetime.today()
        print('\nprofile_database')
        print('\tprofile tables')
        current_table_index = 1
        for table_info_dict in self.table_infos.values():
            print('{} of {} {}'.format(current_table_index, len(self.table_infos), table_info_dict['ansi_full_table_name']))
            current_table_index+=1
            
            table_profile = {}
            table_profile['table_info_id'] = table_info_dict['table_info_id']
            table_profile_sql = 'SELECT COUNT(*) AS "table_row_count" FROM {ansi_full_table_name};'.format(**table_info_dict)
            try:
                start_time = time.perf_counter()
                odbc_cur = self.connection.cursor()
                odbc_cur.execute(table_profile_sql)
                table_row_count = odbc_cur.fetchone()[0]
            except Exception as e:
                print(e)
                print(table_profile_sql)
            finally:
                odbc_cur.close()
            table_row_count_execution_seconds = time.perf_counter() - start_time
            print('\ttable_row_count: {} ({}s)'.format(table_row_count, table_row_count_execution_seconds))
            table_profile['table_row_count'] = table_row_count
            table_profile['table_row_count_execution_seconds'] = table_row_count_execution_seconds
            table_row_count_datetime = profile_datetime
            table_profile['table_row_count_datetime'] = table_row_count_datetime
            """save to logging database with sqlalchemy"""
            table_profile_id = self.profile_saver.log_table_profile(**table_profile)
            table_profile['table_profile_id'] = table_profile_id
            self.table_profiles[table_info_dict['ansi_full_table_name']] = table_profile
        if self.profile_columns:
            print('\n\nprofile columns')
            current_column_index = 1
            for column_info_dict in self.column_infos.values():
                print('\n\t{} of {} {}'.format(current_column_index, len(self.column_infos), column_info_dict['ansi_full_column_name']))
                current_column_index+=1
                ansi_full_table_name = column_info_dict['ansi_full_table_name']
                print(ansi_full_table_name)
                """some records from SQLColumns refer to procs/funcs not tables"""
                if ansi_full_table_name in self.table_profiles:
                    column_profile = {}
                    column_profile['table_profile_id'] = self.table_profiles[ansi_full_table_name]['table_profile_id']
                    column_profile['column_info_id'] = column_info_dict['column_info_id']
                    column_profile_sql = 'SELECT COUNT(DISTINCT {ansi_full_column_name}) AS "column_distinct_row_count" FROM {ansi_full_table_name};'.format(**column_info_dict)
                    try:
                        start_time = time.perf_counter()
                        odbc_cur = self.connection.cursor()
                        odbc_cur.execute(column_profile_sql)
                        column_distinct_count = odbc_cur.fetchone()[0]
                    except Exception as e:
                        print(e)
                        print(column_profile_sql)
                    finally:
                        odbc_cur.close()
                    column_distinct_count_execution_seconds = time.perf_counter() - start_time
                    print('\t\tcolumn_distinct_count: {} ({}s)'.format(column_distinct_count, column_distinct_count_execution_seconds))
                    column_profile['column_distinct_count'] = column_distinct_count
                    column_profile['column_distinct_count_execution_seconds'] = column_distinct_count_execution_seconds
                    column_distinct_count_datetime = profile_datetime
                    column_profile['column_distinct_count_datetime'] = column_distinct_count_datetime
                    """save to logging database with sqlalchemy"""
                    column_profile_id = self.profile_saver.log_column_profile(**column_profile)
                    column_profile['column_profile_id'] = column_profile_id
                    self.column_profiles[column_info_dict['ansi_full_column_name']] = column_profile
                    """group by counts to get frequencies of column values in column (ie histogram)"""
                    if 0 < column_distinct_count <= self.histogram_cutoff:
                        print('\t\t\tcolumn histogram')
                        column_histogram_sql = 'SELECT COUNT(*) AS "column_value_count", {ansi_full_column_name} AS "column_value" FROM {ansi_full_table_name} GROUP BY {ansi_full_column_name}'.format(**column_info_dict)
                        column_histogram = None
                        try:
                            start_time = time.perf_counter()
                            odbc_cur = self.connection.cursor()
                            odbc_cur.execute(column_histogram_sql)
                            column_histogram = list(odbc_cur.fetchall())
                        except Exception as e:
                            print(e)
                            print(column_histogram_sql)
                        finally:
                            odbc_cur.close()
                        column_histogram_execution_seconds = start_time - time.perf_counter()

                        column_histogram_pairs = []
                        for column_histogram_pair in column_histogram:
                            column_histogram_pair_dict = {}
                            column_histogram_pair_dict['column_profile_id'] = column_profile_id
                            column_histogram_pair_dict['column_info_id'] = column_info_dict['column_info_id']
                            column_histogram_pair_dict['column_value_count'] = column_histogram_pair[0]
                            column_histogram_pair_dict['column_value_string'] = column_histogram_pair[1]
                            column_histogram_pairs.append(column_histogram_pair_dict)
                        print('\t\t\tcolumn_histogram_execution_seconds: {}'.format(column_distinct_count_execution_seconds))

                        """save to logging database with sqlalchemy"""
                        column_profile_id = self.profile_saver.log_column_histogram(column_histogram_pairs)


class DenodoProfiler(AutoTestOdbc):
    """derived class for connecting to Denodo with ODBC"""
    def __init__(self, server_name, database_name, port=9999):
        connection_lambda = lambda server_name, database_name, port: "DRIVER={DenodoODBC Unicode(x64)};" + "SERVER={};DATABASE={};UID=admin;PWD=admin;PORT={};".format(dw_denodo_dsn)
        connection_lambda = lambda server_name, database_name: \
            "DRIVER={DenodoODBC Unicode(x64)};" \
            +"SERVER={};DATABASE={};Trusted_Connection=Yes;PORT={};".format(server_name, database_name, port)
        server_type = 'Denodo'
        ansi_column_format = '"{table_owner}"."{table_name}"'
        ansi_column_table_format = '"{table_owner}"."{table_name}"."{column_name}"'
        super(DenodoProfiler, self).__init__(connection_lambda, server_name, database_name, ansi_column_table_format = ansi_column_table_format, ansi_column_format = ansi_column_format)


class SqlServerProfiler(AutoTestOdbc):
    """derived class for connecting to Sql Server with ODBC"""
    def __init__(self, server_name, database_name, driver):
        connection_lambda = lambda server_name, database_name: "DRIVER={"+driver+"};" + "SERVER={};DATABASE={};Trusted_Connection=Yes;".format(server_name, database_name)

        server_type = 'Sql Server'
        ansi_column_format = '"{table_schem}"."{table_name}"'
        ansi_column_table_format = '"{table_schem}"."{table_name}"."{column_name}"'
        super(SqlServerProfiler, self).__init__(connection_lambda, server_name, database_name, ansi_column_table_format = ansi_column_table_format, ansi_column_format = ansi_column_format)
