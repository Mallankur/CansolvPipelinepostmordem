using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace ConsoleApp1.NewFolder1
{
    internal class MongoPipeline
    {
        private IMongoCollection<BsonDocument> collection;

        public MongoPipeline(string connectionString, string databaseName, string collectionName)
        {
            var client = new MongoClient(connectionString);
            var database = client.GetDatabase(databaseName);
            collection = database.GetCollection<BsonDocument>(collectionName);
        }

        public List<MongoDataModel> FetchAndConvertData(DateTime startTime, DateTime endTime, string[] tagNames)
        {
            var filter = Builders<BsonDocument>.Filter.And(
                Builders<BsonDocument>.Filter.In("TagName", tagNames),
                Builders<BsonDocument>.Filter.Gte("EventTime", startTime.ToString("yyyy-MM-ddTHH:mm:ss")),
                Builders<BsonDocument>.Filter.Lte("EventTime", endTime.ToString("yyyy-MM-ddTHH:mm:ss"))
            );

            var documents = collection.Find(filter).ToList();

            var mongoDataList = documents.Select(doc => new MongoDataModel
            {
                _id = doc["_id"].AsObjectId,
                TagName = doc["TagName"].AsString,
                EventTime = DateTime.Parse(doc["EventTime"].AsString, null, DateTimeStyles.RoundtripKind),
                Value = doc["Value"].AsDecimal
            }).ToList();

            return mongoDataList;
        }

        public List<AggregationModel> CalculateAverageForTimeInterval(DateTime startTime, DateTime endTime, string[] tagNames, long frequency)
        {
            var data = FetchAndConvertData(startTime, endTime, tagNames);

            var bsonData = data.Select(d => new BsonDocument
            {
                { "TagName", d.TagName },
                { "EventTime", d.EventTime },
                { "Value", d.Value }
            });

            // Temporary collection name for intermediate data
            var tempCollectionName = "tempDataCollection";
            var tempCollection = collection.Database.GetCollection<BsonDocument>(tempCollectionName);

            // Insert the fetched data into the temporary collection
            tempCollection.InsertMany(bsonData);

            var aggregation = tempCollection.Aggregate()
                .Match(Builders<BsonDocument>.Filter.And(
                    Builders<BsonDocument>.Filter.In("TagName", tagNames),
                    Builders<BsonDocument>.Filter.Gte("EventTime", startTime),
                    Builders<BsonDocument>.Filter.Lte("EventTime", endTime)
                ))
                .Group(new BsonDocument
                {
                    { "_id", new BsonDocument
                        {
                            { "TagName", "$TagName" },
                            { "timeInterval", new BsonDocument("$dateTrunc", new BsonDocument
                                {
                                    { "date", "$EventTime" },
                                    { "unit", "second" },
                                    { "binSize", frequency }
                                })
                            }
                        }
                    },
                    { "averageValue", new BsonDocument("$avg", "$Value") }
                })
                .Project(new BsonDocument
                {
                    { "TagName", "$_id.TagName" },
                    { "EventTime", "$_id.timeInterval" },
                    { "Average", "$averageValue" }
                });

            var results = aggregation.ToList();

            // Drop the temporary collection
            tempCollection.Database.DropCollection(tempCollectionName);

            var aggregationModels = results.Select(result => new AggregationModel
            {
                TagName = result["TagName"].AsString,
                EventTime = result["EventTime"].ToUniversalTime(),
                Average = result["Average"].ToDouble()
            }).ToList();

            return aggregationModels;
        }
    }
    public class AggregationModel
    {
        public string TagName { get; set; }
        public DateTime EventTime { get; set; }
        public double Average { get; set; }
    }
    public class MongoDataModel
    {
        public ObjectId _id { get; set; }
        public string TagName { get; set; }
        public DateTime EventTime { get; set; }
        public decimal Value { get; set; }
    }


}
