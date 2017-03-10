using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Odbc;
using System.Data;

namespace CrossServerProfiler
{
    public class OdbcServerConnection
    {

        protected string connection_string_;
        protected OdbcConnection connection_;
        public OdbcServerConnection(string connection_string)
        {
            connection_string_ = connection_string;
            //Console.WriteLine($"{connection_string_}");
            connection_ = new OdbcConnection(connection_string_);

        }
        protected void Connect()
        {
            if (new List<System.Data.ConnectionState> { System.Data.ConnectionState.Closed, System.Data.ConnectionState.Broken }.Contains(connection_.State))
            {
                connection_.Open();
            }
        }
        protected OdbcCommand CreateCommand()
        {
            Connect();
            return connection_.CreateCommand();
        }
        protected DataTable GetSchema(string collectionName)
        {
            Connect();
            return connection_.GetSchema(collectionName);
        }
        
    }
    public class SqlOdbc : OdbcServerConnection
    {
        protected string server_name_;
        protected string database_name_;
        public SqlOdbc(string connection_string) : base(connection_string)
        {
        }
        public SqlOdbc(string server_name, string database_name) : base($"Driver={{ODBC Driver 11 for SQL Server}};Server={server_name};Database={database_name};Trusted_Connection=Yes;")
        {
            server_name_ = server_name;
            database_name_ = database_name;
        }

        public List<string> Databases()
        {
            Connect();
            string sql = "EXEC sp_databases;";
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
        public List<string> Tables()
        {
            Connect();
            List<string> tables = new List<string>();
            var rdr = GetSchema("Tables").CreateDataReader();
            StringBuilder str = new StringBuilder();
            while (rdr.Read())
            {
                str.Append($"[{rdr.GetString(0)}].[{rdr.GetString(1)}].[{rdr.GetString(2)}]");
                tables.Add(str.ToString());
                str.Clear();
            }
            return tables;
        }
        public SqlOdbc ChangeDatabase(string database_name)
        {
            return new SqlOdbc(server_name_, database_name);
        }
        public Dictionary<string, OdbcCommand> GetRowCountQueries()
        {
            List<string> tables = Tables();
            Dictionary<string, OdbcCommand> rowCountQueries = new Dictionary<string, OdbcCommand>();
            foreach (var table_name in tables)
            {
                var cmd = CreateCommand();
                cmd.CommandText = $"SELECT COUNT(*) AS TableRowCount FROM {table_name};";
                rowCountQueries.Add(table_name, cmd);
            }
            return rowCountQueries;
        }
    }

    public class DenodoOdbc : OdbcServerConnection
    {
        protected string server_name_;
        protected string database_name_;
        protected int port_;
        public DenodoOdbc(string connection_string) : base(connection_string)
        {
        }
        public DenodoOdbc(string server_name, string database_name, int port) : base($"DRIVER={{DenodoODBC Unicode(x64)}};SERVER={server_name};DATABASE={database_name};UID=gcrowell;PWD=gcrowell;PORT={port};")
        {
            server_name_ = server_name;
            database_name_ = database_name;
            port_ = port;
        }

        public List<string> Databases()
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
        public DenodoOdbc ChangeDatabase(string database_name)
        {
            return new DenodoOdbc(server_name_, database_name, port_);
        }
        public Dictionary<string, OdbcCommand> GetRowCountQueries()
        {
            List<string> tables = Tables();
            Dictionary<string, OdbcCommand> rowCountQueries = new Dictionary<string, OdbcCommand>();
            foreach (var table_name in tables)
            {
                var cmd = CreateCommand();
                cmd.CommandText = $"SELECT COUNT(*) AS TableRowCount FROM {table_name};";
                rowCountQueries.Add(table_name, cmd);
            }
            return rowCountQueries;
        }
        
    }


}
