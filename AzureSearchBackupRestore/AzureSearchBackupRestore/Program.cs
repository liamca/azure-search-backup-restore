// This is a prototype tool that allows for extraction of data from an Azure Search index
// Since this tool is still under development, it should not be used for production usage

using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AzureSearchBackupRestore
{
    class Program
    {
        private static string SourceSearchServiceName = [Source Search Service];
        private static string SourceAPIKey = [Source Search Service API Key];
        private static string SourceIndexName = [Source Index Name];
        private static string TargetSearchServiceName = [Target Search Service];
        private static string TargetAPIKey = [Target Search Service API Key];
        private static string TargetIndexName = [Target Index Name];

        private static SearchServiceClient SourceSearchClient;
        private static SearchIndexClient SourceIndexClient;
        private static SearchServiceClient TargetSearchClient;
        private static SearchIndexClient TargetIndexClient;

        private static int MaxBatchSize = 500;          // JSON files will contain this many documents / file and can be up to 1000
        private static int ParallelizedJobs = 10;       // Output content in parallel jobs

        static void Main(string[] args)
        {
            SourceSearchClient = new SearchServiceClient(SourceSearchServiceName, new SearchCredentials(SourceAPIKey));
            SourceIndexClient = SourceSearchClient.Indexes.GetClient(SourceIndexName);
            TargetSearchClient = new SearchServiceClient(TargetSearchServiceName, new SearchCredentials(TargetAPIKey));
            TargetIndexClient = TargetSearchClient.Indexes.GetClient(TargetIndexName);

            // Extract the index schema and write to file
            Console.WriteLine("Writing Index Schema to {0}\r\n", SourceIndexName + ".schema");
            File.WriteAllText(SourceIndexName + ".schema", GetIndexSchema());

            // Extract the content to JSON files 
            int SourceDocCount = GetCurrentDocCount(SourceIndexClient);
            LaunchParallelDataExtraction(SourceDocCount);     // Output content from index to json files

            // Re-create and import content to target index
            DeleteIndex();
            CreateTargetIndex();
            ImportFromJSON();
            Console.WriteLine("\r\nWaiting 10 seconds for target to index content...");
            Console.WriteLine("NOTE: For really large indexes it may take longer to index all content.\r\n");
            Thread.Sleep(10000);

            // Validate all content is in target index
            int TargetDocCount = GetCurrentDocCount(TargetIndexClient);
            Console.WriteLine("Source Index contains {0} docs", SourceDocCount);
            Console.WriteLine("Target Index contains {0} docs\r\n", TargetDocCount);
            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
        }

        static void LaunchParallelDataExtraction(int CurrentDocCount)
        {
            // Launch output in parallel
            string IDFieldName = GetIDFieldName();
            int FileCounter = 0;
            for (int batch = 0; batch <= (CurrentDocCount / MaxBatchSize); batch += ParallelizedJobs)
            {

                List<Task> tasks = new List<Task>();
                for (int job = 0; job < ParallelizedJobs; job++)
                {
                    FileCounter++;
                    int fileCounter = FileCounter;
                    if ((fileCounter - 1) * MaxBatchSize < CurrentDocCount)
                    {
                        Console.WriteLine("Writing {0} docs to {1}", MaxBatchSize, SourceIndexName + fileCounter + ".json");

                        tasks.Add(Task.Factory.StartNew(() =>
                            ExportToJSON((fileCounter - 1) * MaxBatchSize, IDFieldName, SourceIndexName + fileCounter + ".json")
                        ));
                    }

                }
                Task.WaitAll(tasks.ToArray());  // Wait for all the stored procs in the group to complete
            }

            return;
        }

        static void ExportToJSON(int Skip, string IDFieldName, string FileName)
        {
            // Extract all the documents from the selected index to JSON files in batches of 500 docs / file
            string json = string.Empty;
            try
            {
                SearchParameters sp = new SearchParameters()
                {
                    SearchMode = SearchMode.All,
                    Top = MaxBatchSize,
                    Skip = Skip
                };
                DocumentSearchResult response = SourceIndexClient.Documents.Search("*", sp);

                foreach (var doc in response.Results)
                {
                    json += JsonConvert.SerializeObject(doc.Document) + ",";
                    // Geospatial is formatted such that it needs to be changed for reupload
                    // Unfortunately since it comes down in Lat, Lon format, I need to alter it to Lon, Lat for upload

                    while (json.IndexOf("CoordinateSystem") > -1)
                    {
                        // At this point the data looks like this
                        // {"Latitude":38.3399,"Longitude":-86.0887,"IsEmpty":false,"Z":null,"M":null,"CoordinateSystem":{"EpsgId":4326,"Id":"4326","Name":"WGS84"}}
                        int LatStartLocation = json.IndexOf("\"Latitude\":");
                        LatStartLocation = json.IndexOf(":", LatStartLocation) + 1;
                        int LatEndLocation = json.IndexOf(",", LatStartLocation);
                        int LonStartLocation = json.IndexOf("\"Longitude\":");
                        LonStartLocation = json.IndexOf(":", LonStartLocation)+1;
                        int LonEndLocation = json.IndexOf(",", LonStartLocation);
                        string Lat = json.Substring(LatStartLocation, LatEndLocation - LatStartLocation);
                        string Lon = json.Substring(LonStartLocation, LonEndLocation - LonStartLocation);

                        // Now it needs to look like this
                        // { "type": "Point", "coordinates": [-122.131577, 47.678581] }
                        int GeoStartPosition = json.IndexOf("\"Latitude\":") - 1;
                        int GeoEndPosition = json.IndexOf("}}", GeoStartPosition) + 2;
                        string updatedJson = json.Substring(0, GeoStartPosition) + "{ \"type\": \"Point\", \"coordinates\": [";
                        updatedJson += Lon + ", " + Lat + "] }";
                        updatedJson += json.Substring(GeoEndPosition);
                        json = updatedJson;
                    }

                    json = json.Replace("\"Latitude\":", "\"type\": \"Point\", \"coordinates\": [");
                    json = json.Replace("\"Longitude\":", "");
                    json = json.Replace(",\"IsEmpty\":false,\"Z\":null,\"M\":null,\"CoordinateSystem\":{\"EpsgId\":4326,\"Id\":\"4326\",\"Name\":\"WGS84\"}", "]");
                    json += "\r\n";

                    //{ "type": "Point", "coordinates": [-122.131577, 47.678581] }
                    //{"Latitude":41.113,"Longitude":-95.6269}
                    //json += "\r\n";

                }

                // Output the formatted content to a file
                json = json.Substring(0, json.Length - 3); // remove trailing comma
                File.WriteAllText(FileName, "{\"value\": [");
                File.AppendAllText(FileName, json);
                File.AppendAllText(FileName, "]}");
                Console.WriteLine("Total documents written: {0}", response.Results.Count.ToString());
                json = string.Empty;


            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message.ToString());
            }
            return;
        }

        static string GetIDFieldName()
        {
            // Find the id field of this index
            string IDFieldName = string.Empty;
            try
            {
                var schema = SourceSearchClient.Indexes.Get(SourceIndexName);
                foreach (var field in schema.Fields)
                {
                    if (field.IsKey)
                    {
                        IDFieldName = Convert.ToString(field.Name);
                        break;
                    }
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message.ToString());
            }
            return IDFieldName;
        }

        static string GetIndexSchema()
        {
            // Extract the schema for this index
            // I like using REST here since I can just take the response as-is

            Uri ServiceUri = new Uri("https://" + SourceSearchServiceName + ".search.windows.net");
            HttpClient HttpClient = new HttpClient();
            HttpClient.DefaultRequestHeaders.Add("api-key", SourceAPIKey);

            string Schema = string.Empty;
            try
            {
                Uri uri = new Uri(ServiceUri, "/indexes/" + SourceIndexName);
                HttpResponseMessage response = AzureSearchHelper.SendSearchRequest(HttpClient, HttpMethod.Get, uri);
                AzureSearchHelper.EnsureSuccessfulSearchResponse(response);
                Schema = response.Content.ReadAsStringAsync().Result.ToString();

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message.ToString());
            }

            return Schema;
        }

        private static bool DeleteIndex()
        {
            // Delete the index if it exists
            try
            {
                TargetSearchClient.Indexes.Delete(TargetIndexName);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error deleting index: {0}\r\n", ex.Message);
                Console.WriteLine("Did you remember to set your SearchServiceName and SearchServiceApiKey?\r\n");
                return false;
            }

            return true;
        }

        static void CreateTargetIndex()
        {
            // Use the schema file to create a copy of this index
            // I like using REST here since I can just take the response as-is

            string json = File.ReadAllText(SourceIndexName + ".schema");

            // Do some cleaning of this file to change index name, etc
            json = "{" + json.Substring(json.IndexOf("\"name\""));
            int indexOfIndexName = json.IndexOf("\"",json.IndexOf("name\"")+5) + 1;
            int indexOfEndOfIndexName = json.IndexOf("\"",indexOfIndexName);
            json = json.Substring(0, indexOfIndexName) + TargetIndexName + json.Substring(indexOfEndOfIndexName);

            Uri ServiceUri = new Uri("https://" + TargetSearchServiceName + ".search.windows.net");
            HttpClient HttpClient = new HttpClient();
            HttpClient.DefaultRequestHeaders.Add("api-key", TargetAPIKey);

            try
            {
                Uri uri = new Uri(ServiceUri, "/indexes");
                HttpResponseMessage response = AzureSearchHelper.SendSearchRequest(HttpClient, HttpMethod.Post, uri, json);
                response.EnsureSuccessStatusCode();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message.ToString());
            }

        }



        static int GetCurrentDocCount(SearchIndexClient IndexClient)
        {
            // Get the current doc count of the specified index
            try
            {
                SearchParameters sp = new SearchParameters()
                {
                    SearchMode = SearchMode.All,
                    IncludeTotalResultCount = true
                };

                DocumentSearchResult response = IndexClient.Documents.Search("*", sp);
                return Convert.ToInt32(response.Count);

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message.ToString());
            }

            return -1;

        }

        static void ImportFromJSON()
        {
            // Take JSON file and import this as-is to target index
            Uri ServiceUri = new Uri("https://" + TargetSearchServiceName + ".search.windows.net");
            HttpClient HttpClient = new HttpClient();
            HttpClient.DefaultRequestHeaders.Add("api-key", TargetAPIKey);

            try
            {
                foreach (string fileName in Directory.GetFiles(Directory.GetCurrentDirectory(), SourceIndexName +"*.json"))
                {
                    Console.WriteLine("Uploading documents from file {0}", fileName);
                    string json = File.ReadAllText(fileName);
                    Uri uri = new Uri(ServiceUri, "/indexes/"+ TargetIndexName + "/docs/index");
                    HttpResponseMessage response = AzureSearchHelper.SendSearchRequest(HttpClient, HttpMethod.Post, uri, json);
                    response.EnsureSuccessStatusCode();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message.ToString());
            }
        }
    }
}
