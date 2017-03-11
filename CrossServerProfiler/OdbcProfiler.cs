using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Text;

namespace CrossServerProfiler
{
    public interface IDatabaseConnection
    {
        void Connect();
        OdbcCommand CreateCommand();
        OdbcCommand CreateCommand(string command_string);
        DataTable GetSchema(string collection_name);
        //IDatabaseProfile ConnectToDatabase(string database_name);
        List<string> Databases();
        List<string> Tables();
        Dictionary<string, IDatabaseProfile> ConnectToAllDatabases();
        string GetServerName();
        string GetServerType();
        string GetDatabaseName();
        void Close();
    }
    public interface IDatabaseProfile : IDatabaseConnection
    {
        Dictionary<string, OdbcCommand> GenerateRowCountQueries();
    }
    public abstract class DatabaseConnection : IDatabaseProfile
    {
        protected string odbc_connection_string_;
        protected OdbcConnection connection_;
        public DatabaseConnection(string connection_string)
        {
            odbc_connection_string_ = connection_string;
        }
        public void Connect()
        {
            if (connection_ == null)
            {
                connection_ = new OdbcConnection(odbc_connection_string_);
            }
            if (new List<System.Data.ConnectionState> { System.Data.ConnectionState.Closed, System.Data.ConnectionState.Broken }.Contains(connection_.State))
            {
                connection_.Open();
            }
        }
        public void Close()
        {
            if (connection_ != null)
            {
                connection_.Close();
            }
        }
        public abstract string GetServerName();
        public abstract string GetDatabaseName();
        //public IDatabaseProfile ConnectToDatabase(string database_name)
        //{
        //    return new DenodoConnection(server_name_, database_name, port_);
        //}
        public OdbcCommand CreateCommand()
        {
            Connect();
            return connection_.CreateCommand();
        }
        public OdbcCommand CreateCommand(string command_string)
        {
            Connect();
            OdbcCommand cmd = connection_.CreateCommand();
            cmd.CommandText = command_string;
            return cmd;
        }
        public abstract Dictionary<string, IDatabaseProfile> ConnectToAllDatabases();
        public DataTable GetSchema(string collection_name)
        {
            Connect();
            return connection_.GetSchema(collection_name);
        }
        public abstract List<string> Tables();
        public abstract List<string> Databases();
        public abstract string GetServerType();
        public Dictionary<string, OdbcCommand> GenerateRowCountQueries()
        {
            Connect();
            List<string> tables = Tables();
            Dictionary<string, OdbcCommand> rowCountQueries = new Dictionary<string, OdbcCommand>();
            foreach (var table_name in tables)
            {
                rowCountQueries.Add(table_name, CreateCommand(ProfileHelper.GenerateRowCountQuery(table_name)));
            }
            return rowCountQueries;
        }
    }
    public static class ProfileHelper
    {
        public static string GenerateRowCountQuery(string table_name)
        {
            return $"SELECT COUNT(*) FROM {table_name};";
        }

    }

    //public abstract class DatabaseProfile : DatabaseConnection, IDatabaseProfile
    //{
    //    protected string server_name_;
    //    protected string database_name_;
    //    protected int port_;
    //    public DatabaseProfile(string connection_string) : base(connection_string)
    //    {

    //    }
    //    public override abstract string GetServerName();
    //    public override abstract string GetDatabaseName();
    //    public override abstract Dictionary<string, OdbcCommand> GenerateRowCountQueries()
    //    {
    //        Connect();
    //        List<string> tables = Tables();
    //        Dictionary<string, OdbcCommand> rowCountQueries = new Dictionary<string, OdbcCommand>();
    //        foreach (var table_name in tables)
    //        {
    //            rowCountQueries.Add(table_name, CreateCommand(ProfileHelper.GenerateRowCountQuery(table_name)));
    //        }
    //        return rowCountQueries;
    //    }
    //}

    public class DenodoConnection : DatabaseConnection
    {
        protected string server_name_;
        protected string database_name_;
        protected int port_;
        public DenodoConnection(string server_name, string database_name, int port = 9996) : base($"DRIVER={{DenodoODBC Unicode(x64)}};SERVER={server_name};DATABASE={database_name};UID=gcrowell;PWD=gcrowell;PORT={port};")
        {
            server_name_ = server_name;
            database_name_ = database_name;
            port_ = port;
            connection_ = null;
        }
        public override List<string> Tables()
        {
            Connect();
            List<string> tables = new List<string>();
            var rdr = GetSchema("Tables").CreateDataReader();
            StringBuilder str = new StringBuilder();
            while (rdr.Read())
            {
                str.Append($"\"{rdr.GetString(0)}\".\"{rdr.GetString(2)}\"");
                tables.Add(str.ToString());
                str.Clear();
            }
            return tables;
        }
        public override List<string> Databases()
        {
            Connect();
            string sql = "SELECT DISTINCT DATABASE_NAME FROM CATALOG_VDP_METADATA_VIEWS();";
            var cmd = CreateCommand();
            cmd.CommandText = sql;
            var rdr = cmd.ExecuteReader();
            List<string> databases = new List<string>();
            while (rdr.Read())
            {
                databases.Add(rdr.GetValue(0) as string);
            }
            return databases;
        }
        public override string GetDatabaseName()
        {
            return database_name_;
        }
        public override string GetServerName()
        {
            return server_name_;
        }
        public override string GetServerType()
        {
            return "Denodo";
        }
        public override Dictionary<string, IDatabaseProfile> ConnectToAllDatabases()
        {
            List<string> database_names = Databases();
            Dictionary<string, IDatabaseProfile> database_connections = new Dictionary<string, IDatabaseProfile>();

            foreach (string database_name in database_names)
            {
                database_connections.Add(database_name, new DenodoConnection(server_name_, database_name, port_));
            }
            return database_connections;
        }
    }
    public class SqlServerConnection : DatabaseConnection
    {
        protected string server_name_;
        protected string database_name_;
        protected int port_;
        public SqlServerConnection(string server_name, string database_name) : base($"Driver={{ODBC Driver 11 for SQL Server}};Server={server_name};Database={database_name};Trusted_Connection=Yes;")
        {
            server_name_ = server_name;
            database_name_ = database_name;
            connection_ = null;
        }
        public override List<string> Databases()
        {
            Connect();
            List<string> databases = new List<string>();

            string sql;
            sql = "EXEC master.sys.sp_databases"; // no permissions
            sql = "EXEC sp_MSforeachdb 'SELECT ''?''';"; // permission work around
            var cmd = CreateCommand();
            cmd.CommandText = sql;
            var rdr = cmd.ExecuteReader();
            while (true)
            {
                while (rdr.Read())
                {
                    databases.Add(rdr.GetValue(0) as string);
                }
                if(!rdr.NextResult())
                {
                    break;
                }
            }
            Console.WriteLine(databases.Count);
            return databases;
        }
        public override List<string> Tables()
        {
            Connect();
            List<string> tables = new List<string>();
            var rdr = GetSchema("Tables").CreateDataReader();
            StringBuilder str = new StringBuilder();
            while (rdr.Read())
            {
                str.Append($"\"{rdr.GetString(0)}\".\"{rdr.GetString(1)}\".\"{rdr.GetString(2)}\"");
                tables.Add(str.ToString());
                str.Clear();
            }
            return tables;
        }
        public override string GetDatabaseName()
        {
            return database_name_;
        }
        public override string GetServerName()
        {
            return server_name_;
        }
        public override string GetServerType()
        {
            return "SqlServer";
        }
        public override Dictionary<string, IDatabaseProfile> ConnectToAllDatabases()
        {
            List<string> database_names = Databases();
            Dictionary<string, IDatabaseProfile> database_connections = new Dictionary<string, IDatabaseProfile>();

            foreach (string database_name in database_names)
            {
                database_connections.Add(database_name, new SqlServerConnection(server_name_, database_name));
            }
            return database_connections;
        }
    }


    public class Profiler
    {
        protected string server_name_;
        protected string database_name_;
        protected string odbc_connection_string_;
        protected OdbcConnection connection_;
        private DateTime profile_date_;
        public Profiler(string server_name, string database_name)
        {
            server_name_ = server_name;
            database_name_ = database_name;
            odbc_connection_string_ = $"Driver={{ODBC Driver 11 for SQL Server}};Server={server_name_};Database={database_name_};Trusted_Connection=Yes;";
            connection_ = null;
            profile_date_ = DateTime.Now;
        }
        public void Connect()
        {
            if (connection_ == null)
            {
                connection_ = new OdbcConnection(odbc_connection_string_);
            }
            if (new List<System.Data.ConnectionState> { System.Data.ConnectionState.Closed, System.Data.ConnectionState.Broken }.Contains(connection_.State))
            {
                connection_.Open();
            }
        }
        public void Close()
        {
            if (connection_ != null)
            {
                connection_.Close();
            }
        }
        public OdbcCommand CreateCommand()
        {
            Connect();
            return connection_.CreateCommand();
        }
        public OdbcCommand CreateCommand(string command_string)
        {
            Connect();
            OdbcCommand cmd = connection_.CreateCommand();
            cmd.CommandText = command_string;
            return cmd;
        }
        public int LogViewTableRowCount(string server_name, string server_type, string database_name, string table_name, int row_count)
        {
            string sql = @"{CALL dbo.uspInsViewTableProfile(?, ?, ?, ?, ?, ?, ?) }";
            OdbcCommand cmd = CreateCommand(sql);

            var ServerName = cmd.Parameters.Add("@ServerName", OdbcType.VarChar, 100);
            ServerName.Value = server_name;
            var ServerType = cmd.Parameters.Add("@ServerType", OdbcType.VarChar, 100);
            ServerType.Value = server_type;
            var DatabaseName = cmd.Parameters.Add("@DatabaseName", OdbcType.VarChar, 100);
            DatabaseName.Value = database_name;
            var PhysicalViewTableName = cmd.Parameters.Add("@PhysicalViewTableName", OdbcType.VarChar, 100);
            PhysicalViewTableName.Value = table_name;
            var RowCount = cmd.Parameters.Add("@RowCount", OdbcType.Int);
            RowCount.Value = row_count;
            var ViewTableProfileId = cmd.Parameters.Add("@ViewTableProfileId", OdbcType.Int);
            ViewTableProfileId.Direction = ParameterDirection.Output;
            ViewTableProfileId.IsNullable = false;
            var ProfileIsoDateStr = cmd.Parameters.Add("@ProfileIsoDateStr", OdbcType.VarChar, 23);
            string customFormat = "yyyy-MM-ddTHH:mm:ss.fff";
            ProfileIsoDateStr.Value = profile_date_.ToString(customFormat);
            Console.WriteLine($"ProfileIsoDateStr: {ProfileIsoDateStr.Value}");


            cmd.ExecuteNonQuery();
            int ViewTableProfileIdInt = ViewTableProfileId.Value == null ? 0 : (int)ViewTableProfileId.Value;

            return ViewTableProfileIdInt;
        }
        public void ExecuteProfile(IDatabaseProfile server_profile)
        {
            string server_name = server_profile.GetServerName();
            string server_type = server_profile.GetServerType();
            string database_name = server_profile.GetDatabaseName();
            Dictionary<string, OdbcCommand> profile_queries = server_profile.GenerateRowCountQueries();
            foreach (var table_name in profile_queries.Keys)
            {
                Console.WriteLine($"profiling: {server_name}.{database_name}.{table_name} ({server_type})");
                OdbcCommand cmd = profile_queries[table_name];
                try
                {
                    var rdr = cmd.ExecuteReader();
                    rdr.Read();
                    int row_count = rdr.GetInt32(0);
                    LogViewTableRowCount(server_name, server_type, database_name, table_name, row_count);
                }
                catch (Exception e)
                {
                    Console.WriteLine(new String('-',50)+"\n" + e.Message + new String('-', 50) + "\n");
                    Console.WriteLine(e.Message);
                }
                
            }
            server_profile.Close();
        }
        public void ExecuteAllDatabaseProfiles(IDatabaseProfile server_profile)
        {
            Dictionary<string, IDatabaseProfile> database_connections = server_profile.ConnectToAllDatabases();
            foreach (var database_name in database_connections.Keys)
            {
                ExecuteProfile(database_connections[database_name]);
            }
            server_profile.Close();
        }


    }

}