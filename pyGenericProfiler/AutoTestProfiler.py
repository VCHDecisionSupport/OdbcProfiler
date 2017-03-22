from AutoTestOdbc import *
from AutoTestObjectRelationalMapper import *



def main():
    deploy_sql_alchemy_model_database()
    server_name = 'PC'
    database_name = 'WideWorldImportersDW'
    mssql_profiler = SqlServerProfiler(server_name, database_name)
    mssql_profiler.process_meta_data()
    mssql_profiler.profile_database()
if __name__ == '__main__':
    main()