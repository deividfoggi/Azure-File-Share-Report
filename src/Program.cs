using System;
using System.Collections.Generic;
using System.IO;
using Azure.Storage.Files.Shares;
using Azure.Storage.Files.Shares.Models;

namespace AzureFileShareReport
{
    class Program
    {
        // ...existing code...

        static void Main(string[] args)
        {
            Console.Write("Enter the Azure Storage connection string: ");
            string connectionString = Console.ReadLine();
            Console.Write("Enter the Azure File Share name: ");
            string shareName = Console.ReadLine();
            
            string fileName = string.Format("results_{0}.csv", DateTime.Now.ToString("yyyyMMdd_HHmmss"));
            List<string> oldDirectories = new List<string>();
            
            // Write CSV header
            File.WriteAllText(fileName, "ShareName,Directory,File Name,Size (bytes),Last Modified,Created\n");

            ShareClient share = new ShareClient(connectionString, shareName);
            foreach (var x in share.GetRootDirectoryClient().GetFilesAndDirectories())
            {
                ShareDirectoryClient shareDirectoryClient = share.GetDirectoryClient(x.Name);
                long fileSizes = 0;

                foreach (var file in shareDirectoryClient.GetFilesAndDirectories())
                {
                    try
                    {
                        ShareFileClient fileClient = shareDirectoryClient.GetFileClient(file.Name);
                        if(file.IsDirectory)
                        {
                            continue;
                        }
                        ShareFileProperties fileProp = fileClient.GetProperties().Value;
                        fileSizes += (long)fileProp.ContentLength;

                        // Write directly to CSV file
                        File.AppendAllText(fileName, 
                            $"{shareName},{x.Name},{file.Name},{fileProp.ContentLength}," +
                            $"{fileProp.SmbProperties.FileLastWrittenOn?.ToString("dd/MM/yyyy HH:mm:ss")}," +
                            $"{fileProp.SmbProperties.FileCreatedOn?.ToString("dd/MM/yyyy HH:mm:ss")}\n");

                        Console.WriteLine($"{shareName},{file.Name},{fileProp.ContentLength},{fileProp.SmbProperties.FileLastWrittenOn?.ToString("dd/MM/yyyy HH:mm:ss")},{fileProp.SmbProperties.FileCreatedOn?.ToString("dd/MM/yyyy HH:mm:ss")}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error accessing file {file.Name}: {ex.Message}");
                        break;
                    }
                }
            }

            Console.WriteLine($"CSV file exported as: {fileName}");
        }
    }
}