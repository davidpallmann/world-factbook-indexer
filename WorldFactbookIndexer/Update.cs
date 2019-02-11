using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using PolyCloud.Storage.NetCore;
using Newtonsoft.Json.Linq;

namespace WorldFactbookIndexer
{
    public static class Update
    {
        // Data source URLs

        const String DownloadUrl = "https://github.com/iancoleman/cia_world_factbook_api/raw/master/data/factbook.json";
        const String FlagUrlFormat = "https://www.cia.gov/library/publications/resources/the-world-factbook/attachments/flags/{0}-flag.gif";
        const String MapUrlFormat = "https://www.cia.gov/library/publications/resources/the-world-factbook/attachments/maps/{0}-map.gif";

        // Storage account - set to your storage account. 

        const String StorageName = "your-storage-account-name";
        const String StorageKey = "...your-storage-access-key";
        const String ContainerName = "data";

        // Cosmos DB

        private const string EndpointUrl = "https://your-cosmo-db.documents.azure.com:443/";
        private const string PrimaryKey = "...your-cosmo-db-access-key...";
        private static DocumentClient client = new DocumentClient(new Uri(EndpointUrl), PrimaryKey);
        private static Uri countryUrl = UriFactory.CreateDocumentCollectionUri("Factbook", "Country");

        // Global variables

        private static JObject data = null;                     // JSON country data
        private static Country[] countries = Country.List();    // Master country list

        #region Start Function

        // Start function for timer trigger
        // CRON expressions cheat sheet: https://codehollow.com/2017/02/azure-functions-time-trigger-cron-cheat-sheet/

        const String EverySaturday = "0 0 0 * * 5";     // every Saturday
        const String EveryHour = "0 2 * * * *";         // every hour on the hour

        [FunctionName("Update_TimerStart")]
        public static async Task Update_TimerStart([TimerTrigger(EveryHour)]TimerInfo timerInfo,
            [OrchestrationClient] DurableOrchestrationClient starter, ILogger log)
        {
            string instanceId = await starter.StartNewAsync("Update", null);
            log.LogInformation($"================ Started orchestration with ID = '{instanceId}'. at " + DateTime.Now.ToString());
        }

        // Start function for HTTP trigger - used during initial development & running locally to debug

        //[FunctionName("Update_HttpStart")]
        //public static async Task<HttpResponseMessage> HttpStart(
        //    [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]HttpRequestMessage req,
        //    [OrchestrationClient]DurableOrchestrationClient starter,
        //    ILogger log)
        //{
        //    // Function input comes from the request content.
        //    string instanceId = await starter.StartNewAsync("Update", null);

        //    log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

        //    return starter.CreateCheckStatusResponse(req, instanceId);
        //}

        #endregion

        #region Update

        // Update : Azure Durable Function for loading/updating Azure storage and database with World Factbook data

        [FunctionName("Update")]
        public static async Task<List<bool>> RunOrchestrator([OrchestrationTrigger] DurableOrchestrationContext context)
        {
            DurableOrchestrationContext Context = context;

            var outputs = new List<bool>();

            // Download bulk dataset for all countries 

            bool haveData = await context.CallActivityAsync<bool>("DownloadData", "unused");
            outputs.Add(haveData);

            if (haveData)
            {
                // Use fan-out / fan-in to store a dataset for each country

                var tasks = new Task<bool>[countries.Length];
                for (int i = 0; i < countries.Length; i++)
                {
                    tasks[i] = context.CallActivityAsync<bool>("UploadCountryData", countries[i]);
                }
                await Task.WhenAll(tasks);
                foreach(Task<bool> t in tasks)
                {
                    outputs.Add(t.Result);
                }
                outputs.Add(true);
            }

            return outputs;
        }

        #endregion

        #region DownloadData

        // DownloadData : download World Factbook JSON data file

        [FunctionName("DownloadData")]
        public static bool DownloadData([ActivityTrigger] string name, ILogger log)
        {
            try
            {
                // Download factbook.json

                log.LogInformation("Downloading data from World Factbook github");

                String filename = "factbook.json";

                using (WebClient web = new WebClient())
                {
                    web.DownloadFile(DownloadUrl, filename);
                }
                String json = File.ReadAllText(filename);
                data = JObject.Parse(json);

                DeleteTempFile(filename);
                
                log.LogInformation("End function DownloadData (success)");
                return true;
            }
            catch (Exception ex)
            {
                log.LogError("DownloadData: exception - " + ex.Message);
                return false;
            }
        }

        #endregion

        #region UploadCountryData

        // UploadCountryData : upload World Factbook JSON dataset for one country to blob storage and Cosmos DB

        [FunctionName("UploadCountryData")]
        public static async Task<bool> UploadCountryData([ActivityTrigger] Country country, ILogger log)
        {
            log.LogInformation("---- " + country.Name + ": UploadCountryData spawned for country " + country.Name);

            JToken token = null;
            String json = null;

            try
            {
                token = data["countries"][country.Key]["data"];
                json = token.ToString();
            }
            catch (Exception ex)
            {
                log.LogInformation("***** Country key : " + country.Key + " not found in JSON data");
                log.LogError(ex, "UploadCountryData(" + country.Key + ").blob-upload failed");
                return false;
            }

            PartitionKey parkey = new PartitionKey(country.Name);

            if (!UploadCountryData_UploadJson(log, country, json)) return false;
            if (!await UploadCountryData_DeletePriorRecords(log, country, parkey)) return false;
            if (!await UploadCountryData_AddRecord(log, country, parkey, token)) return false;
            if (!UploadCountryData_UploadFlagImage(log, country)) return false;
            if (!UploadCountryData_UploadMapImage(log, country)) return false;
            return true;
        }

        #region UploadCountryData subroutines

        #region UploadCountryData_UploadJson

        // Upload <country>.json to blob storage

        private static bool UploadCountryData_UploadJson(ILogger log, Country country, String json)
        {
            try
            {
                String filename = country.Key + ".json";

                log.LogInformation($"Uploading country data to blob " + filename);

                File.WriteAllText(filename, json);

                using (Storage storage = Storage.Azure(StorageName, StorageKey))
                {
                    storage.Open();
                    storage.UploadFile("data", filename);
                    storage.Close();
                }

                DeleteTempFile(filename);

                return true;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "UploadCountryData(" + country.Key + ").blob-upload failed");
                return false;
            }
        }

        #endregion

        #region UploadCountryData_DeletePriorRecords

        // If country record already exists in Country collection, remove it

        private static async Task<bool> UploadCountryData_DeletePriorRecords(ILogger log, Country country, PartitionKey parkey)
        {
            // If country record already exists, remove it

            log.LogInformation("---- " + country.Key + ": deleting prior Country DB records...");

            var query = new SqlQuerySpec("SELECT * FROM Country c WHERE c.name = @name",
                           new SqlParameterCollection(new SqlParameter[] { new SqlParameter { Name = "@name", Value = country.Name } }));
            var existingCountryRecords = client.CreateDocumentQuery<Microsoft.Azure.Documents.Document>(countryUrl, query, new FeedOptions() { PartitionKey = parkey });

            bool queryDone = false;
            int tries = 0;

            if (existingCountryRecords != null)
            {
                int count = 0;
                queryDone = false;
                tries = 0;

                while (!queryDone)
                {
                    try
                    {
                        tries++;
                        RequestOptions options = new RequestOptions() { PartitionKey = parkey };
                        if (existingCountryRecords != null)
                        {
                            foreach (Microsoft.Azure.Documents.Document c in existingCountryRecords)
                            {
                                //log.LogInformation("     Deleting found document SelfLink: " + c.SelfLink + ", country: " + c.GetPropertyValue<String>("name"));
                                await client.DeleteDocumentAsync(c.SelfLink, options);
                                count++;
                            }
                            if (count>0) log.LogInformation("---- " + country.Key + ": prior record deleted");
                        }
                        queryDone = true;
                        return true;
                    }
                    catch (DocumentClientException de)
                    {
                        var statusCode = (int)de.StatusCode;
                        if ((statusCode == 429 || statusCode == 503) && tries < 3)
                        {
                            log.LogInformation(">>>> Error 429/503(de): " + country.Key + " : RETRYING DELETE AFTER SLEEP <<<<");
                            Thread.Sleep(de.RetryAfter);
                        }
                        else
                        {
                            log.LogError(de, "UploadCountryData(" + country.Key + ").db-delete failed");
                            queryDone = true;   // exit loop because of hard failure
                            return false;
                        }
                    }
                    catch (System.AggregateException ae)
                    {
                        // See if a request rate too large occurred

                        if (ae.InnerException.GetType() == typeof(DocumentClientException))
                        {
                            var docExcep = ae.InnerException as DocumentClientException;
                            var statusCode = (int)docExcep.StatusCode;
                            if ((statusCode == 429 || statusCode == 503) && tries < 3)
                            {
                                log.LogInformation(">>>> Error 429/503(ae): " + country.Key + " : RETRYING DELETE AFTER SLEEP <<<<");
                                Thread.Sleep(docExcep.RetryAfter);
                            }
                            else
                            {
                                log.LogError(ae, "UploadCountryData(" + country.Key + ").db-delete db failed");
                                queryDone = true;   // exit loop because of hard failure
                                return false;
                            }
                        }
                    }
                } // end while !queryDone
            } // end if records-to-delete

            return true;
        }

        #endregion

        #region UploadCountryData_AddRecord

        // Add record to Country collection

        private static async Task<bool> UploadCountryData_AddRecord(ILogger log, Country country, PartitionKey parkey, JToken token)
        {
            bool queryDone = false;
            int tries = 0;

            while (!queryDone)
            {
                try
                {
                    log.LogInformation("---- " + country.Key + ": adding database record");
                    // UriFactory.CreateDocumentCollectionUri("Factbook", "Country")
                    Document doc = await client.CreateDocumentAsync(countryUrl, token, new RequestOptions() { PartitionKey = parkey });
                    log.LogInformation("Country record " + country.Key + " created");
                    queryDone = true;
                }
                catch (DocumentClientException de)
                {
                    var statusCode = (int)de.StatusCode;
                    //if ((statusCode == 409 && tries < 3)
                    //{
                    //    log.LogInformation(">>>> Error 429/503(de): " + country.Key + " : DOCUMENT EXISTS - TRYING UPDATE <<<<");
                    //    client.ReplaceDocumentAsync(countryUrl, )
                    //}
                    if ((statusCode == 429 || statusCode == 503) && tries < 3)
                    {
                        log.LogInformation(">>>> Error 429/503(de): " + country.Key + " : RETRYING ADD AFTER SLEEP <<<<");
                        Thread.Sleep(de.RetryAfter);
                    }
                    else
                    {
                        log.LogError(de, "UploadCountryData(" + country.Key + ").db-add failed");
                        queryDone = true;   // exit loop because of hard failure
                        return false;
                    }
                }
                catch (System.AggregateException ae)
                {
                    // See if a request rate too large occurred

                    if (ae.InnerException.GetType() == typeof(DocumentClientException))
                    {
                        var docExcep = ae.InnerException as DocumentClientException;
                        var statusCode = (int)docExcep.StatusCode;
                        if ((statusCode == 429 || statusCode == 503) && tries < 3)
                        {
                            log.LogInformation(">>>> Error 429/503(ae): " + country.Key + " : RETRYING ADD AFTER SLEEP <<<<");
                            Thread.Sleep(docExcep.RetryAfter);
                        }
                        else
                        {
                            log.LogError(ae, "UploadCountryData(" + country.Key + ").db-add db failed");
                            queryDone = true;   // exit loop because of hard failure
                            return false;
                        }
                    }
                }
            } // end while !queryDone
            return true;
        }

        #endregion

        #region UploadCountryData_UploadFlagImage

        // Download country flag image and upload to blob storage as <country>.gif. Only do this if blob does not already exist.

        private static bool UploadCountryData_UploadFlagImage(ILogger log, Country country)
        {
            using (Storage storage = Storage.Azure(StorageName, StorageKey))
            {
                String flagUrl = String.Format(FlagUrlFormat, country.Code.ToUpper());
                String flagFilename = country.Key + ".gif";

                if (!storage.FileExists(ContainerName, flagFilename))
                {
                    try
                    {
                        // Download flag image file

                        log.LogInformation($"Downloading country flag from " + flagUrl);
                        using (WebClient web = new WebClient())
                        {
                            web.DownloadFile(flagUrl, flagFilename);
                        }

                        // Upload to blob storage

                        log.LogInformation($"Uploading country flag to blob " + flagFilename);

                        storage.Open();
                        storage.UploadFile("data", flagFilename);
                        storage.Close();

                        DeleteTempFile(flagFilename);
                    }
                    catch (Exception ex)
                    {
                        // Flag image not available
                        log.LogError(ex, "UploadCountryData(" + country.Key + ").download-flag failed");
                        return false;
                    }
                }
                return true;
            }
        }

        #endregion
 
        #region UploadCountryData_UploadMapImage

        // Download country map image and upload to blob storage as <country>.gif. Only do this if blob does not already exist.

        private static bool UploadCountryData_UploadMapImage(ILogger log, Country country)
        {
            using (Storage storage = Storage.Azure(StorageName, StorageKey))
            {
                String mapUrl = String.Format(MapUrlFormat, country.Code.ToUpper());
                String mapFilename = country.Key + "-map.gif";

                if (!storage.FileExists(ContainerName, mapFilename))
                {
                    try
                    {
                        // Download flag image file

                        log.LogInformation($"Downloading country map from " + mapUrl);
                        using (WebClient web = new WebClient())
                        {
                            web.DownloadFile(mapUrl, mapFilename);
                        }

                        // Upload to storage

                        log.LogInformation($"Uploading country map to blob " + mapFilename);

                        storage.Open();
                        storage.UploadFile("data", mapFilename);
                        storage.Close();

                        DeleteTempFile(mapFilename);
                    }
                    catch (Exception ex)
                    {
                        // Map image not available
                        log.LogError(ex, "UploadCountryData(" + country.Key + ").download-map failed");
                        return false;
                    }
                }
                return true;
            }
        }

        #endregion

        #endregion

        #endregion

        #region DeleteTempFile

        // Delete a temporary disk that is no longer needed

        private static void DeleteTempFile(String filename)
        {
            // Clean up

            try
            {
                File.Delete(filename);
            }
            catch (IOException)
            {
                // Can't delete download file - not fatal
            }
        }

        #endregion

    }
}