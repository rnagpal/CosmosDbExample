using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;

namespace CosmosTest
{
    class Program
    {
        private static DocumentClient client;
        private static string _databaseId = "";
        private static string _collectionId = "";
        private static string _assetKey = "";
        private const string CosmosConnectionString = "";
        private const string CosmosPrimaryKey = "";

        static void Main(string[] args)
        {
            ConnectionPolicy connectionPolicy = new ConnectionPolicy { ConnectionProtocol = Protocol.Tcp, ConnectionMode = ConnectionMode.Direct };

            client = new DocumentClient(new Uri(CosmosConnectionString), CosmosPrimaryKey, connectionPolicy);

            client.OpenAsync().Wait();

            Stopwatch sw = new Stopwatch();
            sw.Start();

            Console.WriteLine("Starting test1...");

            var test1 = OneQueryMethod(_assetKey); 

            Console.WriteLine("Elapsed time(in sec) for test1 : {0}", sw.ElapsedMilliseconds / 1000);

            sw.Restart();

            Console.WriteLine("Starting test2...");

            var test2 = TwoQueryMethod(_assetKey); 

            Console.WriteLine("Elapsed time(in sec) for test2 : {0}", sw.ElapsedMilliseconds / 1000);

            sw.Restart();

            Console.WriteLine("Starting test3...");

            var test3 = ThreeQueryMethod(_assetKey); 

            Console.WriteLine("Elapsed time(in ms) for test3 : {0}", sw.ElapsedMilliseconds);

            Console.ReadKey();
        }

        public static Dictionary<string, string> OneQueryMethod(string assetKey)
        {
            var queryOptions = new FeedOptions { MaxItemCount = -1, EnableCrossPartitionQuery = true };

            List<Document> documents = client.CreateDocumentQuery(
                    UriFactory.CreateDocumentCollectionUri(_databaseId, _collectionId), //"Select *",
                    queryOptions).ToList();

            List<Document> assetDocuments = documents.FindAll(x => x.GetPropertyValue<string>("Asset_Key") == assetKey).OrderByDescending(x => x.Timestamp).ToList();

            if (!assetDocuments.Any())
            {
                return new Dictionary<string, string>();
            }
            Document latestAssetData = GetLatestAssetData(assetDocuments);

            return JsonConvert.DeserializeObject<Dictionary<string, string>>(latestAssetData.ToString());
        }

        public static Dictionary<string, string> TwoQueryMethod(string assetKey)
        {
            var queryOptions = new FeedOptions { MaxItemCount = -1, EnableCrossPartitionQuery = true };

            var basicDevice = client.CreateDocumentQuery<BasicDevice>(UriFactory.CreateDocumentCollectionUri(_databaseId, _collectionId), queryOptions)
                .Where(x => x.Asset_Key == assetKey)
                .ToList();

            if (!basicDevice.Any())
            {
                return new Dictionary<string, string>();
            }
            var device = basicDevice.OrderByDescending(x => x._ts).First();

            Document doc = client.CreateDocumentQuery(UriFactory.CreateDocumentCollectionUri(_databaseId, _collectionId),
                    queryOptions).Where(x => x.Id == device.Id).ToList().First();

            string iotHubData = doc.GetPropertyValue<object>("IoTHub")?.ToString();
            doc.SetPropertyValue("IoTHub", iotHubData);

            return JsonConvert.DeserializeObject<Dictionary<string, string>>(doc.ToString());
        }

        public static Dictionary<string, string> ThreeQueryMethod(string assetKey)
        {
            var queryOptions = new FeedOptions { MaxItemCount = -1, PartitionKey = new PartitionKey(assetKey) };

            var query = client.CreateDocumentQuery(UriFactory.CreateDocumentCollectionUri(_databaseId, _collectionId),
                    queryOptions).OrderByDescending(x => x.Timestamp).Take(1);

            Document doc = query.AsEnumerable().FirstOrDefault();

            if (doc == null)
            {
                return new Dictionary<string, string>();
            }

            string iotHubData = doc.GetPropertyValue<object>("IoTHub")?.ToString();
            doc.SetPropertyValue("IoTHub", iotHubData);

            return JsonConvert.DeserializeObject<Dictionary<string, string>>(doc.ToString());
        }

        private static Document GetLatestAssetData(IEnumerable<Document> assetDocuments)
        {
            Document latestAssetData = assetDocuments.First();
            string iotHubData = latestAssetData.GetPropertyValue<object>("IoTHub")?.ToString();
            latestAssetData.SetPropertyValue("IoTHub", iotHubData);
            return latestAssetData;
        }
    }
    public class BasicDevice
    {
        public string Asset_Key { get; set; }
        public string Id { get; set; }
        public int _ts { get; set; }
    }
}
