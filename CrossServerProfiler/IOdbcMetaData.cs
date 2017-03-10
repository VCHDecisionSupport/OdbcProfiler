using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Text;

namespace CrossServerProfiler
{
    public interface IDatabaseProfile
    {
        void Connect();
        OdbcCommand CreateCommand();
        OdbcCommand CreateCommand(string command_string);
        DataTable GetSchema(string collectionName);
        IDatabaseProfile ConnectToDatabase(string database_name);
        List<string> Databases();
        List<string> Tables();
        Dictionary<string, IDatabaseProfile> ConnectToAllDatabases();
        Dictionary<string, OdbcCommand> GenerateRowCountQueries();
        string GetServerName();
        string GetServerType();
        string GetDatabaseName();
        void Close();

    }
    public static class ProfileHelper
    {
        public static string GenerateRowCountQuery(string table_name)
        {
            return $"SELECT COUNT(*) FROM {table_name};";
        }
    }

    public abstract class DatabaseProfile : IDatabaseProfile
    {
        protected string odbc_connection_string_;
        protected OdbcConnection connection_;
        protected string server_name_;
        protected string database_name_;
        protected int port_;
        public DatabaseProfile(string connection_string)
        {

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
        public string GetServerName()
        {
            return server_name_;
        }
        public string GetServerType()
        {
            return "Denodo";
        }
        public string GetDatabaseName()
        {
            return database_name_;
        }
        public IDatabaseProfile ConnectToDatabase(string database_name)
        {
            return new DenodoConnection(server_name_, database_name, port_);
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
        public Dictionary<string, IDatabaseProfile> ConnectToAllDatabases()
        {
            List<string> database_names = Databases();
            Dictionary<string, IDatabaseProfile> database_connections = new Dictionary<string, IDatabaseProfile>();

            foreach (string database_name in database_names)
            {
                database_connections.Add(database_name, new DenodoConnection(server_name_, database_name, port_));
            }
            return database_connections;
        }
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
        public DataTable GetSchema(string collectionName)
        {
            Connect();
            return connection_.GetSchema(collectionName);
        }
        public List<string> Tables()
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
        public abstract List<string> Databases();
    }

    public class DenodoConnection : DatabaseProfile
    {
        protected string server_name_;
        protected string database_name_;
        protected int port_;
        protected string odbc_connection_string_;
        protected OdbcConnection connection_;

        public DenodoConnection(string server_name, string database_name, int port = 9996) : base($"DRIVER={{DenodoODBC Unicode(x64)}};SERVER={server_name};DATABASE={database_name};UID=gcrowell;PWD=gcrowell;PORT={port};")
        {
            server_name_ = server_name;
            database_name_ = database_name;
            port_ = port;
            odbc_connection_string_ = $"DRIVER={{DenodoODBC Unicode(x64)}};SERVER={server_name_};DATABASE={database_name_};UID=gcrowell;PWD=gcrowell;PORT={port_};";
            connection_ = null;
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
        public string GetServerName()
        {
            return server_name_;
        }
        public string GetServerType()
        {
            return "Denodo";
        }
        public string GetDatabaseName()
        {
            return database_name_;
        }
        public IDatabaseProfile ConnectToDatabase(string database_name)
        {
            return new DenodoConnection(server_name_, database_name, port_);
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
        public Dictionary<string, IDatabaseProfile> ConnectToAllDatabases()
        {
            List<string> database_names = Databases();
            Dictionary<string, IDatabaseProfile> database_connections = new Dictionary<string, IDatabaseProfile>();

            foreach (string database_name in database_names)
            {
                database_connections.Add(database_name, new DenodoConnection(server_name_, database_name, port_));
            }
            return database_connections;
        }
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
        public DataTable GetSchema(string collectionName)
        {
            Connect();
            return connection_.GetSchema(collectionName);
        }
        public List<string> Tables()
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
                var rdr = cmd.ExecuteReader();
                rdr.Read();
                int row_count = rdr.GetInt32(0);
                LogViewTableRowCount(server_name, server_type, database_name, table_name, row_count);
            }
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


    }

}