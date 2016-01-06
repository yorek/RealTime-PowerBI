using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ConsolePusher.PowerBI.Service;

namespace ConsolePusher
{
    class Program
    {
        static void Main(string[] args)
        {
            RunDemo().Wait();
        }

        public static async Task RunDemo()
        {
            string clientId = "--- Your Client ID ---";

            PowerBIServiceHelper pbi = new PowerBIServiceHelper(clientId);

            Console.WriteLine("Authenticating...");
            pbi.Authenticate();

            Console.WriteLine("Getting Datasets...");
            JObject joDatasets = await pbi.GetDatasets();
            var joDataset = (from d in joDatasets["value"] where d["name"].ToString() == "Sales" select d).FirstOrDefault();

            string datasetId = string.Empty;
            
            if (joDataset != null)
            {
                Console.WriteLine("Sales Dataset found.");
                //Console.WriteLine(joDataset.ToString());
                datasetId = joDataset["id"].ToString();
            } 
            else
            {
                Console.WriteLine("Creating Sales Dataset...");
                string jsonDataset = @"
                    {
                        'name': 'Sales', 
                        'tables': [
                            {
                                'name': 'Product', 
                                'columns': [                                    
                                    { 'name': 'Name', 'dataType': 'string'}, 
                                    { 'name': 'Category', 'dataType': 'string'},
                                    { 'name': 'Quantity', 'dataType': 'Int64'},
                                    { 'name': 'Amount', 'dataType': 'Double'},
                                    { 'name': 'SoldDate', 'dataType': 'DateTime'}
                                ]
                            }
                        ]
                    }";
                JObject joNewDataset = await pbi.CreateDataset(JObject.Parse(jsonDataset), true);
                //Console.WriteLine(joNewDataset);
                datasetId = joDataset["id"].ToString();
            }
            Console.WriteLine("DatasetId = " + datasetId);

            Console.WriteLine("Sending Rows...");
            List<string> rows = new List<string>();
            rows.Add(@"'Name':'Laptop','Category':'Computers', 'Quantity': 1, 'Amount': 600.50");
            rows.Add(@"'Name':'Desktop','Category':'Computers', 'Quantity': 1, 'Amount': 420.25");
            rows.Add(@"'Name':'Headphones','Category':'Audio', 'Quantity': 1, 'Amount': 120.25");
            rows.Add(@"'Name':'LCD Tv','Category':'TV', 'Quantity': 1, 'Amount': 420.75");

            Random rnd = new Random();
            for (int i = 0; i < 1000; i++ )
            {
                int r = rnd.Next(4);
                int d = rnd.Next(7);
                DateTime dt = DateTime.Now.AddDays(-d);
                JObject joRow = JObject.Parse(@"{'rows':[{" + rows[r] + ", 'SoldDate': '" + dt.ToString("yyyy-MM-dd") + "'}]}");

                await pbi.AddRows(datasetId, "Product", joRow);
            }

            Console.WriteLine("Press enter to end app.");
            Console.ReadLine();
        }        
    }
}
