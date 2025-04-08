using System;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Files.Shares;
using Azure.Storage.Files.Shares.Models;

namespace AzureFileShareReport
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.Write("Enter the Azure Storage connection string: ");
            string connectionString = Console.ReadLine();
            Console.Write("Enter the Azure File Share name: ");
            string shareName = Console.ReadLine();

            Stopwatch stopwatch = Stopwatch.StartNew();

            string fileName = string.Format("results_{0}.csv", DateTime.Now.ToString("yyyyMMdd_HHmmss"));
            File.WriteAllText(fileName, "ShareName,Directory,File Name,Size (bytes),Last Write,Last Modified,Created On\n");

            ShareClient share = new ShareClient(connectionString, shareName);
            ShareDirectoryClient rootDirectory = share.GetRootDirectoryClient();

            await ProcessDirectoryAsync(rootDirectory, shareName, fileName);

            stopwatch.Stop();    

            Console.WriteLine($"CSV file exported as: {fileName}");
            Console.WriteLine($"Total time taken: {stopwatch.Elapsed.TotalSeconds} seconds");
        }

        static readonly SemaphoreSlim TaskSemaphore = new SemaphoreSlim(10000); // Limit to 100 concurrent tasks

        static async Task ProcessDirectoryAsync(ShareDirectoryClient directoryClient, string shareName, string fileName)
        {
            var items = directoryClient.GetFilesAndDirectoriesAsync();
            var tasks = new List<Task>();

            await foreach (var item in items)
            {
                await TaskSemaphore.WaitAsync(); // Limit concurrency
                try
                {
                    if (item.IsDirectory)
                    {
                        ShareDirectoryClient subDirectoryClient = directoryClient.GetSubdirectoryClient(item.Name);
                        tasks.Add(Task.Run(async () =>
                        {
                            await ProcessDirectoryAsync(subDirectoryClient, shareName, fileName);
                            TaskSemaphore.Release();
                        }));
                    }
                    else
                    {
                        tasks.Add(Task.Run(async () =>
                        {
                            await ProcessFileAsync(directoryClient, item, shareName, fileName);
                            TaskSemaphore.Release();
                        }));
                    }
                }
                catch
                {
                    TaskSemaphore.Release();
                    throw;
                }
            }

            await Task.WhenAll(tasks);
        }

        static readonly SemaphoreSlim FileWriteSemaphore = new SemaphoreSlim(1, 1);

        static async Task ProcessFileAsync(ShareDirectoryClient directoryClient, ShareFileItem item, string shareName, string fileName)
        {
            List<string> fileDetailsBatch = new List<string>();
            try
            {
                ShareFileClient fileClient = directoryClient.GetFileClient(item.Name);
                ShareFileProperties fileProp = (await fileClient.GetPropertiesAsync()).Value;

                // Construct the directory path
                string directoryPath = string.IsNullOrEmpty(directoryClient.Path) ? "/" : $"/{directoryClient.Path}";

                string fileDetails = $"{shareName},{directoryPath},{item.Name},{fileProp.ContentLength}," +
                                    $"{fileProp.SmbProperties.FileLastWrittenOn?.ToString("dd/MM/yyyy HH:mm:ss")}," +
                                    $"{fileProp.SmbProperties.FileChangedOn?.ToString("dd/MM/yyyy HH:mm:ss")}," +
                                    $"{fileProp.SmbProperties.FileCreatedOn?.ToString("dd/MM/yyyy HH:mm:ss")}";

                // Print file details to the console
                Console.WriteLine(fileDetails);

                if (!string.IsNullOrWhiteSpace(fileDetails))
                {
                    fileDetailsBatch.Add(fileDetails);
                }

                if (fileDetailsBatch.Count >= 1000) // Write in batches of 1000
                {
                    await WriteToFileAsync(fileName, fileDetailsBatch);
                    fileDetailsBatch.Clear();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error accessing file {item.Name}: {ex.Message}");
            }

            if (fileDetailsBatch.Count > 0)
            {
                await WriteToFileAsync(fileName, fileDetailsBatch); // Write any remaining files
            }
        }

        static async Task WriteToFileAsync(string fileName, List<string> fileDetailsBatch)
        {
            await FileWriteSemaphore.WaitAsync(); // Ensure only one task writes to the file at a time
            try
            {
                await File.AppendAllLinesAsync(fileName, fileDetailsBatch);
            }
            finally
            {
                FileWriteSemaphore.Release();
            }
        }
    }
}