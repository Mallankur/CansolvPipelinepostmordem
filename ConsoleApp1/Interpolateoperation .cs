using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    public  class InterpolateOperation
    {
        private IMongoCollection<BsonDocument> collection;
        public InterpolateOperation(string connectionString, string databaseName, string collectionName)
        {
            var client = new MongoClient(connectionString);
            var database = client.GetDatabase(databaseName);
            collection = database.GetCollection<BsonDocument>(collectionName);
        }

        public List<AggregationModel> CalculateAverageForTimeInterval(DateTime startTime, DateTime endTime, string[] tagNames, long frequency)
        {
            var filter = Builders<BsonDocument>.Filter.And(
                Builders<BsonDocument>.Filter.In("TagName", tagNames),
                Builders<BsonDocument>.Filter.Gte("EventTime", startTime.ToString("yyyy-MM-ddTHH:mm:ss")),
                Builders<BsonDocument>.Filter.Lte("EventTime", endTime.ToString("yyyy-MM-ddTHH:mm:ss"))
            );

            var documents = collection.Find(filter).ToList();

            var interpolationModels = documents.Select(doc => new InterpolationModel
            {
                TagName = doc["TagName"].AsString,
                EventTime = DateTime.Parse(doc["EventTime"].AsString),
                Value = double.Parse(doc["Value"].ToString())
            }).ToList();

            var groupedData = interpolationModels
                .GroupBy(im => new
                {
                    im.TagName,
                    timeInterval = new DateTime(((im.EventTime.Ticks / frequency) * frequency))
                })
                .Select(g => new AggregationModel
                {
                    TagName = g.Key.TagName,
                    EventTime = g.Key.timeInterval,
                    Average = g.Average(im => im.Value)
                })
                .ToList();

            return groupedData;
        }
    }
    public class InterpolationModel
    {
        public string TagName { get; set; }
        public DateTime EventTime { get; set; }
        public double Value { get; set; }
    }

    public class AggregationModel
    {
        public string TagName { get; set; }
        public DateTime EventTime { get; set; }
        public double Average { get; set; }
    }
}

