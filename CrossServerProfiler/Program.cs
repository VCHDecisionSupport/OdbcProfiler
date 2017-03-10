using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CrossServerProfiler
{
    class Program
    {
        static void Main(string[] args)
        {
            string server_name;
            server_name = "STDBDECSUP01";
            server_name = "SPAPPDEN001";
            string database_name;
            database_name = "sandbox_paris";
            int port;
            port = 9996;
            DenodoConnection den = null;
            Profiler profiler = null;
            try
            {
                den = new DenodoConnection(server_name, database_name, port);

                string profile_server, profile_database;
                profile_server = "STDBDECSUP01";
                profile_database = "GenericProfiles";
                profiler = new Profiler(profile_server, profile_database);
                profiler.ExecuteProfile(den);
            }
            finally
            {
                den.Close();
                profiler.Close();
            }
            

            
            Console.WriteLine("done");
            Console.ReadKey();

        }
    }
}
