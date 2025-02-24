using System.IO;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;

namespace KafkaMicroservice
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        string containerName = "documents";
        string sourceFolderName = @"C:\Users\Admin_kamatim\source\repos\KafkaMicroservice\AzureBlobsApp\Source";
        string destFolderName = @"C:\Users\Admin_kamatim\source\repos\KafkaMicroservice\AzureBlobsApp\Destination";

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"Background Task Started.... {DateTime.Now}");

            while (!stoppingToken.IsCancellationRequested)
            {
                var tasks = new List<Func<Task>>
            {
                SourceToDestUpload,
                DestToAzureUpload,
                AzureToDestDownload,
                DeleteAzureBlobs,
                EmailLogs
            };

                //2-minute delay
                for (int i = 0; i < tasks.Count; i++)
                {
                    if (stoppingToken.IsCancellationRequested)
                        return;

                    await tasks[i]();

                    if (i < tasks.Count - 1) 
                    {
                        await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
                    }
                }

                _logger.LogInformation("Pausing for 1 minutes before restarting cycle...");
                await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
            }
        }

        private Task<Object> SourceToDestUpload()
        {
            _logger.LogInformation($"Executing SourceToDestUpload: First task running...{DateTime.Now}");

            string[] allFiles = Directory.GetFiles(sourceFolderName);
            foreach (var file in allFiles)
            {
                var destFileName = destFolderName + Path.GetFileNameWithoutExtension(file) + Path.GetExtension(file);

                if (File.Exists(destFileName))
                {
                    File.Delete(destFileName);
                }

                File.Move(file, destFolderName,true);
            }
            return (Task<object>)Task.CompletedTask;
        }

        private async Task<Object> DestToAzureUpload()
        {
            _logger.LogInformation($"Executing DestToAzureUpload: Second task running...{DateTime.Now}");

            BlobServiceClient blobServiceClient = GetBlobConnection();
            BlobContainerClient blobClient = blobServiceClient.GetBlobContainerClient(containerName);
            string[] allFiles = Directory.GetFiles(sourceFolderName);

            foreach (var filePath in allFiles)
            {
                string fileName = Path.GetFileName(filePath);

                await using FileStream fileStream = File.OpenRead(filePath);
                blobClient.UploadBlob(fileName,fileStream);

                Console.WriteLine($"Uploaded: {fileName}");
            }
            return Task.FromResult("");
        }

        private Task AzureToDestDownload()
        {
            BlobServiceClient blobServiceClient = GetBlobConnection();
            BlobContainerClient blobClient = blobServiceClient.GetBlobContainerClient(containerName);
            var blobls = blobClient.GetBlobs();

            foreach (var blob in blobls)
            {
                Console.WriteLine(blob.Name);
            }
            _logger.LogInformation($"Executing AzureToDestDownload: Third task running...{DateTime.Now}");
            System.Threading.Thread.Sleep( 10000 );
            return Task.CompletedTask;
        }

        private Task DeleteAzureBlobs()
        {
            _logger.LogInformation($"Executing DeleteAzureBlobs: Second task running...{DateTime.Now}");
            return Task.CompletedTask;
        }

        private Task EmailLogs()
        {
            _logger.LogInformation($"Executing EmailLogs: Second task running...{DateTime.Now}");
            return Task.CompletedTask;
        }

        public BlobServiceClient GetBlobConnection()
        {
            string connectionString = "DefaultEndpointsProtocol=https;AccountName=my1309storageacc;AccountKey=D4l1AuFc/sKUQW5YKkPQKtuRc3VKvN8hSbfVNiyELERIiy55UYIl6OO3AjARQAfaWEuZv9SkU0EA+AStkWbNCw==;EndpointSuffix=core.windows.net";

            return new BlobServiceClient(connectionString);
        }
    }

}
