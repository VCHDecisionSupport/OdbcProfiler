using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CrossServerProfiler
{
    class Program
    {
        static void test_ConnectAllDatabases()
        {
            string server_name, database_name;
            string profile_server, profile_database;
            int port;

            DenodoConnection den = null;
            SqlServerConnection sql = null;
            Profiler profiler = null;
            server_name = "STDBDECSUP02";
            database_name = "master";
            sql = new SqlServerConnection(server_name, database_name);
            foreach (var item in sql.Databases())
            {
                Console.WriteLine(item);
            }
            foreach (var item in sql.Tables())
            {
                Console.WriteLine(item);
            }

            //server_name = "SPAPPDEN001";
            //database_name = "sandbox_paris";
            //port = 9996;
            //den = new DenodoConnection(server_name, database_name, port);
            //foreach (var item in den.Databases())
            //{
            //    Console.WriteLine(item);
            //}
            //foreach (var item in den.Tables())
            //{
            //    Console.WriteLine(item);
            //}
        }

        static void ProfileAll()
        {
            string server_name, database_name;
            string profile_server, profile_database;
            int port;

            DenodoConnection den = null;
            SqlServerConnection sql = null;
            Profiler profiler = null;
            try
            {
                profile_server = "STDBDECSUP01";
                profile_database = "GenericProfiles";
                profiler = new Profiler(profile_server, profile_database);


                server_name = "SPAPPDEN001";
                database_name = "sandbox_paris";
                port = 9996;
                den = new DenodoConnection(server_name, database_name, port);
                profiler.ExecuteProfile(den);
                profiler.ExecuteAllDatabaseProfiles(den);


                server_name = "STDBDECSUP02";
                database_name = "CommunityMart";
                sql = new SqlServerConnection(server_name, database_name);
                profiler.ExecuteProfile(sql);

                profiler.ExecuteAllDatabaseProfiles(sql);
            }
            finally
            {
                den.Close();
                profiler.Close();
            }
        }
        static void Main(string[] args)
        {
            ProfileAll();

            Console.WriteLine("done");
            Console.ReadKey();

        }
    }
}
