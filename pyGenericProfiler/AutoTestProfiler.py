from AutoTestOdbc import *
from AutoTestObjectRelationalMapper import *
import time


def main():
    print('\n\n\n{}AutoTestProfiler{}\n'.format('-'*50,'-'*50))
    deploy_sql_alchemy_model_database()
    server_name = 'STDBDECSUP03'
    # server_name = 'PC'
    database_name = 'CommunityMart'
    # database_name = 'WideWorldImportersDW'
    mssql_profiler = SqlServerProfiler(server_name, database_name)
    mssql_profiler.process_meta_data()
    mssql_profiler.profile_database()

if __name__ == '__main__':
    try:
        start = time.perf_counter()
        main()
    except Exception as e:
        print(e)
    finally:
        print('total runtime: {}s'.format(time.perf_counter()-start))
    