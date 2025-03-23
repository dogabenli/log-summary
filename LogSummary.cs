using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;

namespace LogSummaryFunctionApp
{
    public class LogSummary
    {
        private readonly ILogger _logger;

        public LogSummary(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<LogSummary>();
        }

        [Function("LogSummary")]
        public async Task<HttpResponseData> RunAsync(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequestData req)
        {
            _logger.LogInformation($"LogSummary function manually triggered at: {DateTime.UtcNow}");

            try
            {
                string blobConnectionString = Environment.GetEnvironmentVariable("BLOB_CONNECTION_STRING");
                string blobContainerName = Environment.GetEnvironmentVariable("BLOB_CONTAINER_NAME");
                string logFolderName = Environment.GetEnvironmentVariable("BLOB_FOLDER_NAME"); 
                string outputFolderName = Environment.GetEnvironmentVariable("OUTPUT_FOLDER_NAME") ?? "output/";

                if (string.IsNullOrEmpty(blobConnectionString) || string.IsNullOrEmpty(blobContainerName))
                {
                    _logger.LogError("Blob connection string or container name is missing!");
                    var errorResponse = req.CreateResponse(System.Net.HttpStatusCode.InternalServerError);
                    await errorResponse.WriteStringAsync("Missing blob storage configuration.");
                    return errorResponse;
                }

                BlobServiceClient blobServiceClient = new BlobServiceClient(blobConnectionString);
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);

                _logger.LogInformation("Fetching blobs for today's logs...");
                var today = DateTime.UtcNow.ToString("yyyy-MM-dd");
                var blobs = containerClient.GetBlobs()
                    .Where(b => b.Name.StartsWith($"{logFolderName}{today}_"));

                var logsBySource = new Dictionary<string, List<LogRecord>>();

                foreach (var blob in blobs)
                {
                    _logger.LogInformation($"Processing blob: {blob.Name}");
                    BlobClient blobClient = containerClient.GetBlobClient(blob.Name);
                    using var stream = new MemoryStream();
                    await blobClient.DownloadToAsync(stream);
                    stream.Position = 0;

                    using var reader = new StreamReader(stream);
                    using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
                    {
                        PrepareHeaderForMatch = args => args.Header.ToLowerInvariant().Trim()
                    });

                    csv.Context.RegisterClassMap<LogRecordMap>();
                    var records = csv.GetRecords<LogRecord>().ToList();

                    foreach (var record in records)
                    {
                        if (!logsBySource.ContainsKey(record.CustomDimensions_Role))
                        {
                            logsBySource[record.CustomDimensions_Role] = new List<LogRecord>();
                        }
                        logsBySource[record.CustomDimensions_Role].Add(record);
                    }
                }

                if (!logsBySource.Any())
                {
                    _logger.LogWarning("No logs found for today's date.");
                    var noLogsResponse = req.CreateResponse(System.Net.HttpStatusCode.NotFound);
                    await noLogsResponse.WriteStringAsync("No logs found for today's date.");
                    return noLogsResponse;
                }

                foreach (var source in logsBySource.Keys)
                {
                    var hourlySummary = new int[24, 2]; // Array for warnings and errors per hour
                    var warnings = new Dictionary<string, int>();
                    var errors = new Dictionary<string, int>();

                    foreach (var record in logsBySource[source])
                    {
                        if (DateTime.TryParse(record.Timestamp, out var timestamp))
                        {
                            int hour = timestamp.Hour;
                            if (record.SeverityLevel == 2)
                            {
                                hourlySummary[hour, 0]++; // Warnings
                                if (!warnings.ContainsKey(record.Message))
                                    warnings[record.Message] = 0;
                                warnings[record.Message]++;
                            }
                            if (record.SeverityLevel == 3)
                            {
                                hourlySummary[hour, 1]++; // Errors
                                if (!errors.ContainsKey(record.Message))
                                    errors[record.Message] = 0;
                                errors[record.Message]++;
                            }
                        }
                    }

                    // Generate hourly summary file
                    string summaryFileName = $"{outputFolderName}{today}_{source}_summary.csv";
                    BlobClient summaryBlob = containerClient.GetBlobClient(summaryFileName);

                    using var summaryStream = new MemoryStream();
                    using var writer = new StreamWriter(summaryStream);
                    writer.WriteLine("Hour,Warnings,Errors");
                    for (int i = 0; i < 24; i++)
                    {
                        writer.WriteLine($"{i},{hourlySummary[i, 0]},{hourlySummary[i, 1]}");
                    }
                    writer.Flush();
                    summaryStream.Position = 0;

                    await summaryBlob.UploadAsync(summaryStream, overwrite: true);
                    _logger.LogInformation($"Summary file saved to: {summaryFileName}");

                    // Generate top 10 warnings
                    var topWarnings = warnings.OrderByDescending(w => w.Value).Take(10);
                    string warningsFileName = $"{outputFolderName}{today}_{source}_warnings.csv";
                    await GenerateTopMessagesFile(containerClient, warningsFileName, topWarnings);

                    // Generate top 10 errors
                    var topErrors = errors.OrderByDescending(e => e.Value).Take(10);
                    string errorsFileName = $"{outputFolderName}{today}_{source}_errors.csv";
                    await GenerateTopMessagesFile(containerClient, errorsFileName, topErrors);
                }

                var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
                await response.WriteStringAsync("Summary and top messages files created successfully.");
                return response;
            }
            catch (Exception ex)
            {
                _logger.LogError($"An error occurred: {ex.Message}", ex);
                var errorResponse = req.CreateResponse(System.Net.HttpStatusCode.InternalServerError);
                await errorResponse.WriteStringAsync("An error occurred during log processing.");
                return errorResponse;
            }
        }

        private async Task GenerateTopMessagesFile(BlobContainerClient containerClient, string fileName, IEnumerable<KeyValuePair<string, int>> messages)
        {
            BlobClient blobClient = containerClient.GetBlobClient(fileName);
            using var stream = new MemoryStream();
            using var writer = new StreamWriter(stream);
            writer.WriteLine("Message,Count");
            foreach (var message in messages)
            {
                writer.WriteLine($"\"{message.Key}\",{message.Value}");
            }
            writer.Flush();
            stream.Position = 0;
            await blobClient.UploadAsync(stream, overwrite: true);
            _logger.LogInformation($"File saved to: {fileName}");
        }
    }

    public class LogRecord
    {
        public string Timestamp { get; set; }
        public int SeverityLevel { get; set; }
        public string CustomDimensions_Role { get; set; }
        public string Message { get; set; }
        public string CustomDimensions_StackTrace { get; set; }
    }

    public class LogRecordMap : ClassMap<LogRecord>
    {
        public LogRecordMap()
        {
            Map(m => m.Timestamp).Name("timestamp [UTC]");
            Map(m => m.SeverityLevel).Name("severityLevel");
            Map(m => m.CustomDimensions_Role).Name("customDimensions_Role");
            Map(m => m.Message).Name("message");
            Map(m => m.CustomDimensions_StackTrace).Name("customDimensions_StackTrace");
        }
    }
}
