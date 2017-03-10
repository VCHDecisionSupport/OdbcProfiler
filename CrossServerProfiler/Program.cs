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

            DenodoOdbc den = new DenodoOdbc(server_name, database_name, port);
            foreach (var item in den.Databases())
            {
                Console.WriteLine(item);
            }
            foreach (var item in den.Tables())
            {
                Console.WriteLine(item);
            }
            Console.WriteLine("done");
            Console.ReadKey();

        }
    }
}
