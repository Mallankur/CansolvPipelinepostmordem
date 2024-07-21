using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace ConsoleApp1.NewFolder1
{
    public class GenericPipeline
    {
        private IMongoCollection<BsonDocument> collection;

        public GenericPipeline(string connectionString, string databaseName, string collectionName)
        {
            var client = new MongoClient(connectionString);
            var database = client.GetDatabase(databaseName);
            collection = database.GetCollection<BsonDocument>(collectionName);
        }

        public List<MongoDataModel<T>> FetchAndConvertData<T>(DateTime startTime, DateTime endTime, string[] tagNames)
        {
            var filter = Builders<BsonDocument>.Filter.And(
                Builders<BsonDocument>.Filter.In("TagName", tagNames),
                Builders<BsonDocument>.Filter.Gte("EventTime", startTime.ToString("yyyy-MM-ddTHH:mm:ss")),
                Builders<BsonDocument>.Filter.Lte("EventTime", endTime.ToString("yyyy-MM-ddTHH:mm:ss"))
            );

            var documents = collection.Find(filter).ToList();

            if (typeof(T) == typeof(DateTime))
            {
                return documents.Select(doc => new MongoDataModel<T>
                {
                    _id = doc["_id"].AsObjectId,
                    TagName = doc["TagName"].AsString,
                    EventTime = (T)(object)DateTime.Parse(doc["EventTime"].AsString, null, DateTimeStyles.RoundtripKind),
                    Value = (decimal)doc["Value"].ToDouble()
                }).ToList();
            }
            else if (typeof(T) == typeof(string))
            {
                return documents.Select(doc => new MongoDataModel<T>
                {
                    _id = doc["_id"].AsObjectId,
                    TagName = doc["TagName"].AsString,
                    EventTime = (T)(object)doc["EventTime"].AsString,
                    Value = (decimal)doc["Value"].ToDouble(),
                }).ToList();
            }
            else
            {
                throw new NotSupportedException($"Type {typeof(T)} is not supported");
            }
        }

        public List<AggregationModells> CalculateAverageForTimeInterval<T>(DateTime startTime, DateTime endTime, string[] tagNames, long frequency)
        {
            var data = FetchAndConvertData<T>(startTime, endTime, tagNames);

            var bsonData = data.Select(d => new BsonDocument
            {
                { "TagName", d.TagName },
                { "EventTime", typeof(T) == typeof(DateTime) ? (BsonValue)(DateTime)(object)d.EventTime : d.EventTime.ToString() },
                { "Value", d.Value }
            }).ToList();

            var tempCollectionName = "tempDataCollection";
            var tempCollection = collection.Database.GetCollection<BsonDocument>(tempCollectionName);

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

            var aggregationModels = results.Select(result => new AggregationModells
            {
                TagName = result["TagName"].AsString,
                EventTime = result["EventTime"].ToUniversalTime(),
                Average = result["Average"].ToDouble()
            }).ToList();

            return aggregationModels;
        }
    }

    public class MongoDataModel<T>
    {
        public ObjectId _id { get; set; }
        public string TagName { get; set; }
        public T EventTime { get; set; }
        public decimal Value { get; set; }
    }

    public class AggregationModells
    {
        public string TagName { get; set; }
        public DateTime EventTime { get; set; }
        public double Average { get; set; }
    }
}
