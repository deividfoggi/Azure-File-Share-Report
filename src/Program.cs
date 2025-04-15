using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using Azure.Storage.Files.Shares;
using Azure.Storage.Files.Shares.Models;
using System.Net;

namespace AzureFileShareReport
{
    class Program
    {
        #region Performance Configuration Parameters
        // Thread and concurrency settings
        private static readonly int THREAD_POOL_MIN_THREADS = 1000; // Reduced from 8000 (over-allocation)
        private static readonly int CONNECTION_LIMIT = 2000; // Reduced from 10000
        private static readonly int MAX_CONCURRENT_TASKS = 2000; // Reduced from 10000 make sure it is not greater than CONNECTION_LIMIT otherwise it will be limited by the connection limit
        
        // Batch processing settings - increase throughput per batch
        private static readonly int BATCH_INITIAL_CAPACITY = 100000; // Keep as is
        private static readonly int BATCH_FLUSH_THRESHOLD = 100000; // Increased from 10000
        private static readonly int FILE_WRITE_BUFFER_SIZE = 32 * 1024 * 1024; // Increased to 32MB
        
        // Timeout settings
        private static readonly TimeSpan API_TIMEOUT = TimeSpan.FromSeconds(20);
        private static readonly TimeSpan DIRECTORY_LIST_TIMEOUT = TimeSpan.FromSeconds(120);
        private static readonly TimeSpan SEMAPHORE_WAIT_TIMEOUT = TimeSpan.FromMilliseconds(20); // From 5ms
        private static readonly TimeSpan TASK_POLL_INTERVAL = TimeSpan.FromMilliseconds(5); // From 10ms
        
        // Task management - optimize task handling
        private static readonly int TASK_CLEANUP_FREQUENCY = 50; // From 10 
        private static readonly int MAX_SEMAPHORE_RETRIES = 20; // From 50
        private static readonly int MAX_PROCESSING_TIMES = 2000;
        
        // Display settings
        private static readonly TimeSpan DIAGNOSTIC_INTERVAL = TimeSpan.FromSeconds(2);
        private static readonly TimeSpan PROGRESS_UPDATE_INTERVAL = TimeSpan.FromSeconds(2);
        private static readonly TimeSpan SCAN_PROGRESS_UPDATE_INTERVAL = TimeSpan.FromMilliseconds(500);

        private static readonly string ERROR_LOG_FILE = "error_log.txt";
        #endregion
        
        #region Application State Variables
        private static int _totalItems = 0;
        private static int _processedItems = 0;
        private static readonly SemaphoreSlim _progressSemaphore = new SemaphoreSlim(1, 1);
        private static DateTime _lastProgressUpdate = DateTime.MinValue;
        private static int _scanningItems = 0;
        private static int _scannedItems = 0;
        private static DateTime _lastScanProgressUpdate = DateTime.MinValue;
        private static readonly Random _random = new Random();
        private static int _errorCount = 0;
        private static readonly SemaphoreSlim _consoleSemaphore = new SemaphoreSlim(1, 1);
        private static bool _isProgressBarVisible = false;
        private static int _totalApiCalls = 0;
        private static int _apiErrors = 0;
        private static int _filesProcessed = 0;
        private static int _directoriesProcessed = 0;
        private static DateTime _lastDiagnosticUpdate = DateTime.MinValue;
        private static readonly List<double> _processingTimes = new List<double>();
        private static readonly SemaphoreSlim _taskSemaphore = new SemaphoreSlim(MAX_CONCURRENT_TASKS);
        private static readonly List<string> _fileDetailsBatch = new List<string>(BATCH_INITIAL_CAPACITY);
        private static readonly SemaphoreSlim _batchSemaphore = new SemaphoreSlim(1, 1);
        private static readonly SemaphoreSlim _fileWriteSemaphore = new SemaphoreSlim(1, 1);
        private static int _activeThreads = 0;
        #endregion

        static async Task Main(string[] args)
        {
            // Apply system-wide configuration
            ThreadPool.SetMinThreads(THREAD_POOL_MIN_THREADS, THREAD_POOL_MIN_THREADS);
            ServicePointManager.DefaultConnectionLimit = CONNECTION_LIMIT;
            ServicePointManager.Expect100Continue = false;
            ServicePointManager.UseNagleAlgorithm = false;
            
            Console.Write("Enter the Azure Storage connection string: ");
            string connectionString = Console.ReadLine();
            Console.Write("Enter the Azure File Share name: ");
            string shareName = Console.ReadLine();

            Stopwatch stopwatch = Stopwatch.StartNew();

            string fileName = string.Format("results_{0}.csv", DateTime.Now.ToString("yyyyMMdd_HHmmss"));
            File.WriteAllText(fileName, "ShareName,Directory,File Name,Size (bytes),Last Write,Last Modified,Created On\n");

            // Create or clear error log file
            try
            {
                // Use absolute path to avoid any path resolution issues
                string absoluteLogPath = Path.GetFullPath(ERROR_LOG_FILE);
                File.WriteAllText(absoluteLogPath, $"Azure File Share Report Error Log - {DateTime.Now:yyyy-MM-dd HH:mm:ss}{Environment.NewLine}{Environment.NewLine}");
                Console.WriteLine($"Error log will be written to: {absoluteLogPath}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"WARNING: Cannot write to error log file: {ex.Message}");
            }

            ShareClient share = new ShareClient(connectionString, shareName);
            ShareDirectoryClient rootDirectory = share.GetRootDirectoryClient();

            // Add a periodic diagnostic task
            _ = Task.Run(async () =>
            {
                while (true)
                {
                    await LogDiagnosticsAsync();
                    await Task.Delay(5000);
                }
            });

            Console.WriteLine("Scanning file share structure...");
            _scanningItems = 1; // Start with 1 for the root directory
            var scanStopwatch = Stopwatch.StartNew();
            await CountItemsAsync(rootDirectory);
            _scanningItems = 0;
            scanStopwatch.Stop();
            Console.WriteLine($"\nFound {_totalItems} items to process (scanning took {scanStopwatch.Elapsed.TotalSeconds:F2} seconds).");

            // Process files
            Console.WriteLine("Processing files:");
            DrawProgressBar(0);
            _isProgressBarVisible = true;

            await ProcessDirectoryAsync(rootDirectory, shareName, fileName);

            // Write any remaining items in the batch
            await FlushBatchAsync(fileName);

            stopwatch.Stop();    
            
            // Clear progress bar line
            Console.SetCursorPosition(0, Console.CursorTop);
            Console.Write(new string(' ', Console.WindowWidth - 1));
            Console.SetCursorPosition(0, Console.CursorTop);

            Console.WriteLine($"CSV file exported as: {fileName}");
            Console.WriteLine($"Total time taken: {stopwatch.Elapsed.TotalSeconds:F2} seconds");
        }

        private static async Task CountItemsAsync(ShareDirectoryClient directoryClient)
        {
            try
            {
                List<Task> subDirectoryTasks = new List<Task>();
                var items = directoryClient.GetFilesAndDirectoriesAsync();
                
                await foreach (var item in items)
                {
                    Interlocked.Increment(ref _totalItems);
                    Interlocked.Increment(ref _scannedItems);
                    
                    // Update scan progress periodically
                    UpdateScanProgress();
                    
                    if (item.IsDirectory)
                    {
                        ShareDirectoryClient subDirectoryClient = directoryClient.GetSubdirectoryClient(item.Name);
                        
                        // Limit concurrent tasks to avoid memory issues
                        if (subDirectoryTasks.Count >= 20)
                        {
                            var completedTask = await Task.WhenAny(subDirectoryTasks);
                            subDirectoryTasks.Remove(completedTask);
                        }
                        
                        Interlocked.Increment(ref _scanningItems);
                        var subDirTask = Task.Run(async () => 
                        {
                            try { await CountItemsAsync(subDirectoryClient); }
                            finally { Interlocked.Decrement(ref _scanningItems); }
                        });
                        
                        subDirectoryTasks.Add(subDirTask);
                    }
                }
                
                // Wait for all subdirectory tasks
                if (subDirectoryTasks.Count > 0)
                {
                    await Task.WhenAll(subDirectoryTasks);
                }
            }
            catch (Exception ex)
            {
                string errorMessage = $"Error counting items in {directoryClient.Path}: {ex.Message}";
                Console.WriteLine(errorMessage);
                await LogFileProcessingAsync(errorMessage);
            }
        }

        static async Task ProcessDirectoryAsync(ShareDirectoryClient rootDirectory, string shareName, string fileName)
        {
            var directoryQueue = new ConcurrentQueue<ShareDirectoryClient>();
            directoryQueue.Enqueue(rootDirectory);
            
            // Process directories in the queue
            while (!directoryQueue.IsEmpty)
            {
                // Try to get a directory from the queue
                if (!directoryQueue.TryDequeue(out ShareDirectoryClient currentDirectory))
                    continue;
                    
                try
                {
                    // Process all items in this directory
                    using var dirListingCts = new CancellationTokenSource(DIRECTORY_LIST_TIMEOUT);
                    var directoryItems = currentDirectory.GetFilesAndDirectoriesAsync(cancellationToken: dirListingCts.Token);
                    
                    List<Task> fileTasks = new List<Task>();
                    
                    // Process every item in this directory
                    await foreach (var item in directoryItems)
                    {
                        Interlocked.Increment(ref _processedItems);
                        
                        if (item.IsDirectory)
                        {
                            // Add subdirectory to the queue
                            Interlocked.Increment(ref _directoriesProcessed);
                            var subDir = currentDirectory.GetSubdirectoryClient(item.Name);
                            directoryQueue.Enqueue(subDir);
                        }
                        else
                        {
                            // Create a task for processing the file
                            int semaphoreRetries = 0;
                            while (true)
                            {
                                // Try to get a semaphore slot with timeout
                                bool acquired = await _taskSemaphore.WaitAsync(SEMAPHORE_WAIT_TIMEOUT);
                                if (acquired)
                                {
                                    fileTasks.Add(Task.Run(async () =>
                                    {
                                        try
                                        {
                                            Interlocked.Increment(ref _activeThreads);
                                            await ProcessFileAsync(currentDirectory, item, shareName, fileName);
                                        }
                                        finally
                                        {
                                            Interlocked.Decrement(ref _activeThreads);
                                            _taskSemaphore.Release();
                                        }
                                    }));
                                    break;
                                }
                                
                                // If we couldn't get a semaphore slot after several retries, just process synchronously
                                semaphoreRetries++;
                                if (semaphoreRetries >= MAX_SEMAPHORE_RETRIES)
                                {
                                    await ProcessFileAsync(currentDirectory, item, shareName, fileName);
                                    break;
                                }
                                
                                await Task.Delay(5); // Wait a bit before retrying
                            }
                        }
                        
                        // Periodically clean up completed tasks
                        if (fileTasks.Count > 0 && fileTasks.Count % TASK_CLEANUP_FREQUENCY == 0)
                        {
                            fileTasks.RemoveAll(t => t.IsCompleted);
                        }
                    }
                    
                    // Wait for file tasks from this directory to complete
                    while (fileTasks.Count > 0)
                    {
                        await Task.Delay(TASK_POLL_INTERVAL);
                        fileTasks.RemoveAll(t => t.IsCompleted);
                    }
                }
                catch (Exception ex)
                {
                    string errorMessage = $"Error processing directory {currentDirectory.Path}: {ex.Message}";
                    Interlocked.Increment(ref _errorCount);
                    await LogFileProcessingAsync(errorMessage); // Make sure this is called
                }
            }
        }

        static async Task ProcessFileAsync(ShareDirectoryClient directoryClient, ShareFileItem item, string shareName, string fileName)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                // Skip files with zero length if possible
                if (item.FileSize == 0)
                {
                    Interlocked.Increment(ref _filesProcessed);
                    string zeroFileDirPath = string.IsNullOrEmpty(directoryClient.Path) ? "/" : $"/{directoryClient.Path}";
                    string zeroFileDetails = $"{shareName},{zeroFileDirPath},{item.Name},0,,,,";

                    await _batchSemaphore.WaitAsync();
                    try 
                    {
                        _fileDetailsBatch.Add(zeroFileDetails);
                        if (_fileDetailsBatch.Count >= BATCH_FLUSH_THRESHOLD)
                        {
                            await FlushBatchAsync(fileName);
                        }
                    }
                    finally
                    {
                        _batchSemaphore.Release();
                    }
                    return;
                }
                
                ShareFileClient fileClient = directoryClient.GetFileClient(item.Name);
                
                // Get properties with timeout from configuration
                using var cts = new CancellationTokenSource(API_TIMEOUT);
                ShareFileProperties fileProp;
                
                try
                {
                    Interlocked.Increment(ref _totalApiCalls);
                    var apiStopwatch = Stopwatch.StartNew();
                    
                    fileProp = await fileClient.GetPropertiesAsync(cancellationToken: cts.Token);
                    
                    apiStopwatch.Stop();
                    lock(_processingTimes)
                    {
                        _processingTimes.Add(apiStopwatch.Elapsed.TotalMilliseconds);
                    }
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref _apiErrors);
                    if (ex is OperationCanceledException)
                    {
                        string directoryPath = string.IsNullOrEmpty(directoryClient.Path) ? "/" : $"/{directoryClient.Path}";
                        string fileDetails = $"{shareName},{directoryPath},{item.Name},{item.FileSize},,,,,";

                        await _batchSemaphore.WaitAsync();
                        try 
                        {
                            _fileDetailsBatch.Add(fileDetails);
                        }
                        finally 
                        {
                            _batchSemaphore.Release();
                        }
                        return;
                    }
                    throw;
                }

                // Construct the directory path
                string standardDirectoryPath = string.IsNullOrEmpty(directoryClient.Path) ? "/" : $"/{directoryClient.Path}";

                string standardFileDetails = $"{shareName},{standardDirectoryPath},{item.Name},{fileProp.ContentLength}," +
                                     $"{fileProp.SmbProperties.FileLastWrittenOn?.ToString("dd/MM/yyyy HH:mm:ss")}," +
                                     $"{fileProp.SmbProperties.FileChangedOn?.ToString("dd/MM/yyyy HH:mm:ss")}," +
                                     $"{fileProp.SmbProperties.FileCreatedOn?.ToString("dd/MM/yyyy HH:mm:ss")}";

                await _batchSemaphore.WaitAsync();
                try 
                {
                    _fileDetailsBatch.Add(standardFileDetails);
                        
                    // Flush when the batch is full
                    if (_fileDetailsBatch.Count >= BATCH_FLUSH_THRESHOLD)
                    {
                        await FlushBatchAsync(fileName);
                    }
                }
                finally
                {
                    _batchSemaphore.Release();
                }

                Interlocked.Increment(ref _filesProcessed);
            }
            catch (Exception ex)
            {
                string errorMessage = $"Error processing file {item.Name}: {ex.Message}";
                Interlocked.Increment(ref _errorCount);
                
                // Try async first, then sync if that fails
                try
                {
                    await LogFileProcessingAsync(errorMessage);
                }
                catch
                {
                    LogFileProcessingSync(errorMessage);
                }
            }
        }

        static async Task FlushBatchAsync(string fileName)
        {
            if (_fileDetailsBatch.Count == 0) return;
            
            List<string> batchToWrite;
            
            await _batchSemaphore.WaitAsync();
            try 
            {
                batchToWrite = new List<string>(_fileDetailsBatch);
                _fileDetailsBatch.Clear();
            }
            finally
            {
                _batchSemaphore.Release();
            }
            
            await WriteToFileAsync(fileName, batchToWrite);
        }

        static async Task WriteToFileAsync(string fileName, List<string> fileDetailsBatch)
        {
            await _fileWriteSemaphore.WaitAsync();
            try
            {
                using (FileStream fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.None, FILE_WRITE_BUFFER_SIZE))
                using (StreamWriter writer = new StreamWriter(fs, System.Text.Encoding.UTF8))
                {
                    foreach (var line in fileDetailsBatch)
                    {
                        writer.WriteLine(line);
                    }
                }
            }
            finally
            {
                _fileWriteSemaphore.Release();
            }
        }

        private static async Task LogDiagnosticsAsync()
        {
            double avgProcessingTime = 0;
            if (_processingTimes.Count > 0)
            {
                avgProcessingTime = _processingTimes.Sum() / _processingTimes.Count;
                if (_processingTimes.Count > MAX_PROCESSING_TIMES)
                    _processingTimes.RemoveRange(0, _processingTimes.Count - MAX_PROCESSING_TIMES);
            }

            await _consoleSemaphore.WaitAsync();
            try
            {
                DateTime now = DateTime.Now;
                if ((now - _lastDiagnosticUpdate) < DIAGNOSTIC_INTERVAL) return;
                
                _lastDiagnosticUpdate = now;
                
                // Instead of clearing line by line, save cursor position and restore after
                Console.Clear(); // More efficient than clearing line by line
                Console.SetCursorPosition(0, 0);
                
                // Output diagnostics with active threads
                Console.WriteLine("=== DIAGNOSTICS ===");
                Console.WriteLine($"API Calls: {_totalApiCalls}, Errors: {_apiErrors}, Error Rate: {(_apiErrors * 100.0 / Math.Max(1, _totalApiCalls)):F2}%");
                Console.WriteLine($"Files: {_filesProcessed}, Directories: {_directoriesProcessed}, Active Threads: {_activeThreads}");
                Console.WriteLine($"Avg API Time: {avgProcessingTime:F2}ms, Task Semaphore: {_taskSemaphore.CurrentCount}/{MAX_CONCURRENT_TASKS}");
                Console.WriteLine($"Batch Size: {_fileDetailsBatch.Count}/{BATCH_FLUSH_THRESHOLD}, Error Count: {_errorCount}");
                Console.WriteLine($"Thread Pool: Threads={GetActiveThreadCount()}, Available={GetAvailableThreads()}");
                Console.WriteLine("=================");
                
                // Always show progress
                if (_isProgressBarVisible)
                {
                    int percentage = (int)Math.Min(100, Math.Max(0, (_processedItems * 100.0 / _totalItems)));
                    Console.WriteLine($"Progress: {_processedItems}/{_totalItems} items ({percentage}%)");
                    DrawProgressBar(percentage);
                }
            }
            finally
            {
                _consoleSemaphore.Release();
            }
        }

        private static int GetActiveThreadCount()
        {
            ThreadPool.GetMaxThreads(out int maxWorker, out int _);
            ThreadPool.GetAvailableThreads(out int availableWorker, out int _);
            return maxWorker - availableWorker;
        }

        private static int GetAvailableThreads()
        {
            ThreadPool.GetAvailableThreads(out int availableWorker, out int _);
            return availableWorker;
        }

        private static void DrawProgressBar(int percentage)
        {
            int currentLineCursor = Console.CursorTop;
            Console.SetCursorPosition(0, Console.CursorTop);
            
            int width = Console.WindowWidth - 10;
            Console.Write("[");
            
            int fillLength = (int)Math.Floor(percentage * width / 100.0);
            
            if (fillLength > 0)
            {
                Console.Write(new string('=', fillLength));
            }
            
            if (width - fillLength > 0)
            {
                Console.Write(new string(' ', width - fillLength));
            }
            
            Console.Write($"] {percentage}%");
            
            Console.SetCursorPosition(0, currentLineCursor + 1);
        }

        private static void UpdateScanProgress() {/* ... */}
        private static async Task LogFileProcessingAsync(string message)
        {
            await _consoleSemaphore.WaitAsync();
            try
            {
                Console.WriteLine($"[File Processing Log] {message}");
                
                // Log to file with better error handling
                try
                {
                    // Ensure directory exists
                    string directory = Path.GetDirectoryName(ERROR_LOG_FILE);
                    if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                    {
                        Directory.CreateDirectory(directory);
                    }
                    
                    // Use FileStream with proper sharing mode for better reliability
                    using (FileStream fs = new FileStream(ERROR_LOG_FILE, FileMode.Append, FileAccess.Write, FileShare.Read))
                    using (StreamWriter writer = new StreamWriter(fs))
                    {
                        await writer.WriteLineAsync($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}");
                        await writer.FlushAsync();
                    }
                }
                catch (Exception ex)
                {
                    // If logging itself fails, output details to console
                    Console.WriteLine($"Error writing to log file: {ex.GetType().Name}: {ex.Message}");
                }
            }
            finally
            {
                _consoleSemaphore.Release();
            }
        }

        private static void LogFileProcessingSync(string message)
        {
            try
            {
                File.AppendAllText(ERROR_LOG_FILE, $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}{Environment.NewLine}");
            }
            catch
            {
                // Last resort - can't do much if this fails
            }
        }

        private static string FormatFileSize(long bytes)
        {
            if (bytes < 1024) return $"{bytes} B";
            if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F2} KB";
            if (bytes < 1024 * 1024 * 1024) return $"{bytes / (1024.0 * 1024):F2} MB";
            return $"{bytes / (1024.0 * 1024 * 1024):F2} GB";
        }
    }
}