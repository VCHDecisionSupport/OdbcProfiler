from AutoTestOdbc import *
from AutoTestObjectRelationalMapper import *
import time


def main():
    start = time.perf_counter()
    deploy_sql_alchemy_model_database()
    server_name = 'SPDBDECSUP04'
    server_name = 'PC'
    database_name = 'CommunityMart'
    database_name = 'WideWorldImportersDW'
    mssql_profiler = SqlServerProfiler(server_name, database_name)
    # mssql_profiler.process_meta_data()
    # mssql_profiler.profile_database()

    print('total runtime: {}s'.format(time.perf_counter()-start))
if __name__ == '__main__':
    main()