using ConsoleApp1.NewFolder1;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    public  class Program
    {
        static void Main(string[] args)
        {
            var connectionString = "mongodb+srv://dsa:Ankur%40123@ankur.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000";

            var databaseName = "RealTime";
            var collectionName = "CansolveData";

            var genericPipeline = new GenericPipeline(connectionString, databaseName, collectionName);

            DateTime startTime = new DateTime(2024, 07, 16, 16, 05, 00);
            DateTime endTime = new DateTime(2024, 07, 16, 16, 06, 00);
            string[] tagNames = { "AM:SASK:CCS_BDPS-UEGGACT103A-CO" };
            long frequency = 60;  

            var results = genericPipeline.CalculateAverageForTimeInterval<DateTime>(startTime, endTime, tagNames, frequency);

            foreach (var result in results)
            {
                Console.WriteLine($"TagName: {result.TagName}, EventTime: {result.EventTime}, Average: {result.Average}");
            }
            Console.ReadLine();

        }
    }
}
