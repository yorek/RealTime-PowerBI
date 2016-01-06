using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Microsoft.ServiceBus.Messaging;

namespace EventHubSample
{
    class Program
    {
        private static string _eventHubName = "--- Your Event Hub Name ---";
        private static string _connectionString = "--- Your Event Hub Connection String ---";

        static void Main(string[] args)
        {
            Console.WriteLine("Press Ctrl-C to stop the sender process");
            Console.WriteLine("Press Enter to start now");
            Console.ReadLine();
            SendingRandomMessages();
        }

        static void SendingRandomMessages()
        {
            var eventHubClient = EventHubClient.CreateFromConnectionString(_connectionString, _eventHubName);

            List<string> rows = new List<string>();
            rows.Add(@"'Name':'Laptop','Category':'Computers', 'Quantity': 1, 'Amount': 600.50");
            rows.Add(@"'Name':'Desktop','Category':'Computers', 'Quantity': 1, 'Amount': 420.25");
            rows.Add(@"'Name':'Headphones','Category':'Audio', 'Quantity': 1, 'Amount': 120.25");
            rows.Add(@"'Name':'LCD Tv','Category':'TV', 'Quantity': 1, 'Amount': 420.75");

            Random rnd = new Random();
            while (true)
            {
                try
                {
                    int r = rnd.Next(4);
                    string message = @"{" + rows[r] + ", 'SoldDate': '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "'}";

                    Console.WriteLine("{0} > Sending message: {1}", DateTime.Now, message);
                    eventHubClient.Send(new EventData(Encoding.UTF8.GetBytes(message)));
                }
                catch (Exception exception)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("{0} > Exception: {1}", DateTime.Now, exception.Message);
                    Console.ResetColor();
                }

                Thread.Sleep(200);
            }
        }
    }
}
