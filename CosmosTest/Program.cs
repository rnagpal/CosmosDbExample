using System;
using System.Collections.Generic;
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
            client = new DocumentClient(new Uri(CosmosConnectionString), CosmosPrimaryKey);
            var test1 = OneQueryMethod(_assetKey); // Seeing 15-20 second times
            var test2 = TwoQueryMethod(_assetKey); // Seeing 5-12 second times
            Console.Read();
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
