import pyodbc
import operator


class OdbcServerConnection(object):
    def __init__(self, odbc_connection_string):
        self.__source_connection_string = odbc_connection_string
        # self.__server_name = dsn_name
        # self.__source_connection_string = r'DSN={}'.format(self.__server_name)
        # 'DRIVER={ODBC Driver 13 for SQL Server};'+'SERVER={};Trusted_Connection=Yes;'.format(self.__server_name)

        self.__connection = None
        self.__cursors = {}
        self.__info = {}
        print('{}: {}'.format(self.__class__.__name__,
                              self.__source_connection_string))

    def info(self):
        """general ODBC connection info"""
        self.connect()
        self.__info['SQL_SERVER_NAME'] = self.__connection.getinfo(
            pyodbc.SQL_SERVER_NAME)
        self.__info['SQL_DRIVER_ODBC_VER'] = self.__connection.getinfo(
            pyodbc.SQL_DRIVER_ODBC_VER)
        print('ODBC Connection Info (SQLGetInfo)')
        for key, value in self.__info.items():
            print('\t{}: {}'.format(key, value))

    def connect(self):
        """establish ODBC connection"""
        if self.__connection is None:
            print('\tconnecting to: {}'.format(
                self.__source_connection_string))
            try:
                self.__connection = pyodbc.connect(
                    self.__source_connection_string)
            except Exception as e:
                print(e)

    def query(self, query_name, query_str):
        self.connect()
        self.__cursors[query_name] = self.__connection.cursor()
        self.__cursors[query_name].execute(query_str)

    def sql_cmd(self, cmd_name, cmd_str, cmd_params):
        self.connect()
        if isinstance(cmd_params, list):
            if isinstance(cmd_params[0], tuple):
                print('list of tuples -> executemany()')
                self.__cursors[cmd_name] = self.__connection.cursor()
                self.__cursors[cmd_name].executemany(cmd_str, cmd_params)
        if isinstance(cmd_params, tuple):
            print('tuple -> execute()')
            self.__cursors[cmd_name] = self.__connection.cursor()
            self.__cursors[cmd_name].execute(cmd_str, cmd_params)

    def __getitem__(self, key):
        if key in self.__cursors:
            return self.__cursors[key]
        else:
            raise KeyError('there is no query with name: {}'.format(key))

    def __del__(self):
        if self.__connection is not None:
            print('committing changes to: {}'.format(
                self.__source_connection_string))
            self.__connection.commit()
            print('\tclosing {}'.format(self.__source_connection_string))
            self.__connection.close()

    def tables(self):
        """returns pyodbc.cursor of all tables"""
        self.connect()
        cur = self.__connection.cursor()
        return cur.tables()

    def columns(self):
        """returns pyodbc.cursor of all columns"""
        self.connect()
        cur = self.__connection.cursor()
        return cur.columns()


class Schema(OdbcServerConnection):
    def __init__(self, odbc_connection_string):
        self.__odbc_connection_string = odbc_connection_string
        self.__table_name_format = None
        super(Schema, self).__init__(self.__odbc_connection_string)

    def databases(self):
        raise NotImplementedError()


class DenodoSchema(Schema):
    def __init__(self, server_name, database_name):
        self.__server_name = server_name
        self.__database_name = database_name
        self.__source_connection_string = "DRIVER={DenodoODBC Unicode(x64)};" + "SERVER={};DATABASE={};UID=admin;PWD=admin;PORT=9996;".format(
            self.__server_name, self.__database_name)
        print('{}: {}'.format(self.__class__.__name__, self.__server_name))
        super(DenodoSchema, self).__init__(self.__source_connection_string)

    def databases(self):
        self.connect()
        self.__database_query = 'SELECT DISTINCT DATABASE_NAME FROM CATALOG_VDP_METADATA_VIEWS();'
        self.query('databases', self.__database_query)
        return self['databases']

    def server_name(self):
        return self.__server_name

    def connect_database(self, database_name):
        return DenodoSchema(self.__server_name, database_name)


class MsSql(Schema):
    def __init__(self, server_name, database_name=None):
        self.__server_name = server_name
        self.__database_name = database_name
        self.__databases = []
        print('{}: {}'.format(self.__class__.__name__, self.__server_name))
        self.__source_connection_string = 'DRIVER={ODBC Driver 13 for SQL Server};' + \
            'SERVER={};Trusted_Connection=Yes;'.format(self.__server_name)
        if self.__database_name is not None:
            self.__source_connection_string + \
                'DATABASE={};'.format(database_name)
        super(MsSql, self).__init__(self.__source_connection_string)

    def databases(self):
        self.connect()
        self.__database_query = """EXEC sp_MSforeachdb 'SELECT ''?''';"""
        self.query('databases', self.__database_query)
        if len(self.__databases) == 0:
            while self['databases'].nextset():
                for x in self['databases']:
                    self.__databases.append(x)
        return self.__databases

    def server_name(self):
        return self.__server_name

    def connect_database(self, database_name):
        return DenodoSchema(self.__server_name, database_name)


class GenericProfileMsSqlInput(OdbcServerConnection):
    def __init__(self, server_name):
        self.__server_name = server_name
        self.__database_name = 'GenericProfiles'
        self.__source_connection_string = 'DRIVER={ODBC Driver 13 for SQL Server};' + \
            'SERVER={};DATABASE={};Trusted_Connection=Yes;'.format(
                self.__server_name, self.__database_name)
        super(GenericProfileMsSqlInput, self).__init__(
            self.__source_connection_string)

    def populate(self, schema):
        print(schema.__class__.__name__)
        for database in schema.databases():
            print('database: {}'.format(database[0]))
            schema_database = schema.connect_database(database[0])
            print('tables: {}'.format(list(schema_database.tables())))
            

if __name__ == '__main__':
    # con = OdbcServerConnection('PC_Sql')
    # con.info()

    sql = MsSql('PC')
    print(list(sql.databases()))

    den = DenodoSchema('PC', 'admin')
    print(list(den.databases()))

    input = GenericProfileMsSqlInput('PC')
    input.populate(den)

    # print(list(sql.tables()))
    # print(list(sql.columns()))

    # print(list(den.tables()))
    # print(list(den.columns()))


#'SELECT DISTINCT DATABASE_NAME FROM CATALOG_VDP_METADATA_VIEWS();'
#'SELECT DISTINCT VIEW_NAME FROM CATALOG_VDP_METADATA_VIEWS();'
#'SELECT DISTINCT COLUMN_NAME FROM CATALOG_VDP_METADATA_VIEWS();'
