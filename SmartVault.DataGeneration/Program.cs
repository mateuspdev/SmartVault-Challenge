using Dapper;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using SmartVault.Library;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Xml.Serialization;

namespace SmartVault.DataGeneration
{
    partial class Program
    {
        static void Main(string[] args)
        {
            var stopwatch = Stopwatch.StartNew();

            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json").Build();

            SQLiteConnection.CreateFile(configuration["DatabaseFileName"]);

            // Create a test document with more maintainable content
            string testDocPath = "TestDoc.txt";
            using (StreamWriter writer = new StreamWriter(testDocPath))
            {
                string line = "This is my test document";
                // Write the same line 100 times for test data
                for (int i = 0; i < 100; i++)
                {
                    writer.WriteLine(line);
                }
            }

            Console.WriteLine($"Created database at: {Path.GetFullPath(configuration["DatabaseFileName"])}");
            Console.WriteLine($"Created test document at: {Path.GetFullPath(testDocPath)}");

            using (var connection = new SQLiteConnection(string.Format(configuration?["ConnectionStrings:DefaultConnection"] ?? "", configuration?["DatabaseFileName"])))
            {
                connection.Open();

                // Create tables
                var files = Directory.GetFiles(Path.Combine(Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "..", "..", "..", "..")), "BusinessObjectSchema"));
                for (int i = 0; i <= 2; i++)
                {
                    var serializer = new XmlSerializer(typeof(BusinessObject));
                    var businessObject = serializer.Deserialize(new StreamReader(files[i])) as BusinessObject;
                    connection.Execute(businessObject?.Script);
                }

                // Get document file info once instead of 1,000,000 times
                var documentPath = new FileInfo(testDocPath).FullName;
                var documentLength = new FileInfo(documentPath).Length;

                // Use a transaction for better performance
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        // Prepare random date generator for users
                        var randomDayIterator = RandomDay().GetEnumerator();

                        // Get current timestamp for CreatedOn fields
                        string currentTimestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");

                        // Batch insert users and accounts
                        var userInserts = new List<string>();
                        var accountInserts = new List<string>();

                        for (int i = 0; i < 100; i++)
                        {
                            randomDayIterator.MoveNext();
                            var randomDate = randomDayIterator.Current.ToString("yyyy-MM-dd");
                            userInserts.Add($"('{i}','FName{i}','LName{i}','{randomDate}','{i}','UserName-{i}','e10adc3949ba59abbe56e057f20f883e','{currentTimestamp}')");
                            accountInserts.Add($"('{i}','Account{i}','{currentTimestamp}')");
                        }

                        // Execute batch inserts for users and accounts
                        connection.Execute(
                            $"INSERT INTO User (Id, FirstName, LastName, DateOfBirth, AccountId, Username, Password, CreatedOn) VALUES {string.Join(",", userInserts)}",
                            transaction: transaction);

                        connection.Execute(
                            $"INSERT INTO Account (Id, Name, CreatedOn) VALUES {string.Join(",", accountInserts)}",
                            transaction: transaction);

                        // Insert documents in batches to avoid memory issues
                        const int batchSize = 1000;
                        var documentNumber = 0;

                        for (int i = 0; i < 100; i++)
                        {
                            for (int batch = 0; batch < 10; batch++) // 10 batches of 1000 = 10,000 docs per account
                            {
                                var documentInserts = new List<string>();

                                for (int d = 0; d < batchSize; d++, documentNumber++)
                                {
                                    documentInserts.Add($"('{documentNumber}','Document{i}-{batch * batchSize + d}.txt','{documentPath}','{documentLength}','{i}','{currentTimestamp}')");
                                }

                                connection.Execute(
                                    $"INSERT INTO Document (Id, Name, FilePath, Length, AccountId, CreatedOn) VALUES {string.Join(",", documentInserts)}",
                                    transaction: transaction);

                                // Report progress
                                if (documentNumber % 100000 == 0)
                                {
                                    Console.WriteLine($"Inserted {documentNumber} documents...");
                                }
                            }
                        }

                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error during data generation: {ex.Message}");
                        transaction.Rollback();
                        throw;
                    }
                }

                var accountCount = connection.ExecuteScalar<int>("SELECT COUNT(*) FROM Account;");
                Console.WriteLine($"Total accounts created: {accountCount:N0}");

                var documentCount = connection.ExecuteScalar<int>("SELECT COUNT(*) FROM Document;");
                Console.WriteLine($"Total documents created: {documentCount:N0}");

                var userCount = connection.ExecuteScalar<int>("SELECT COUNT(*) FROM User;");
                Console.WriteLine($"Total users created: {userCount:N0}");

                // Verify database structure
                Console.WriteLine("\nVerifying database structure:");

                // Get table schema information
                var tables = new[] { "Account", "Document", "User" };
                foreach (var table in tables)
                {
                    Console.WriteLine($"\nTable: {table}");
                    var columns = connection.Query("PRAGMA table_info(" + table + ");");
                    foreach (var column in columns)
                    {
                        Console.WriteLine($"  {column.name} ({column.type})");
                    }

                    // Show sample data
                    Console.WriteLine("\nSample data:");
                    var sampleData = connection.Query($"SELECT * FROM {table} LIMIT 1");
                    foreach (var row in sampleData)
                    {
                        foreach (var property in ((IDictionary<string, object>)row))
                        {
                            Console.WriteLine($"  {property.Key}: {property.Value}");
                        }
                    }
                }

                stopwatch.Stop();
                Console.WriteLine($"\nData generation completed in {stopwatch.ElapsedMilliseconds / 1000.0} seconds");
            }
        }

        static IEnumerable<DateTime> RandomDay()
        {
            DateTime start = new DateTime(1985, 1, 1);
            Random gen = new Random();
            int range = (DateTime.Today - start).Days;
            while (true)
                yield return start.AddDays(gen.Next(range));
        }
    }
}