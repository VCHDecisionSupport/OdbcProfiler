import time
import argparse
from OdbcProfiler import AutoTestObjectRelationalMapper as orm
from OdbcProfiler import AutoTestOdbc as pro


def cli():

    parser = argparse.ArgumentParser(description='Profile an ODBC data source.')
    parser.add_argument('server_name', type=str, nargs=1,
                        help='server name of database to be profiled')
    parser.add_argument('database_name', type=str, nargs=1,
                        help='database name to be profiled')
    parser.add_argument('odbc_driver', type=str, nargs=1,
                        help='denodo: "DenodoODBC Unicode(x64)" sql server 2016: "ODBC Driver 13 for SQL Server" sql server 2012: "ODBC Driver 13 for SQL Server"')
    args = parser.parse_args()
    return args


def main():
    print('\n\n\n{}AutoTestProfiler{}\n'.format('-'*50, '-'*50))
    # orm.deploy_sql_alchemy_model_database()
    server_name = 'STDBDECSUP03'
    server_name = 'PC'
    database_name = 'CommunityMart'
    database_name = 'WideWorldImportersDW'
    driver = "ODBC Driver 13 for SQL Server"
    mssql_profiler = pro.SqlServerProfiler(server_name, database_name, driver)
    mssql_profiler.process_meta_data()
    mssql_profiler.profile_database()
    return
if __name__ == '__main__':
    try:
        start = time.perf_counter()
        main()
        # cli()
    except Exception as e:
        print(e)
    finally:
        print('total runtime: {}s'.format(time.perf_counter()-start))
