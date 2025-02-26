using Dapper;
using Microsoft.Extensions.Configuration;
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

                        // Batch insert users and accounts
                        var userInserts = new List<string>();
                        var accountInserts = new List<string>();

                        for (int i = 0; i < 100; i++)
                        {
                            randomDayIterator.MoveNext();
                            var randomDate = randomDayIterator.Current.ToString("yyyy-MM-dd");
                            userInserts.Add($"('{i}','FName{i}','LName{i}','{randomDate}','{i}','UserName-{i}','e10adc3949ba59abbe56e057f20f883e')");
                            accountInserts.Add($"('{i}','Account{i}')");
                        }

                        // Execute batch inserts for users and accounts
                        connection.Execute(
                            $"INSERT INTO User (Id, FirstName, LastName, DateOfBirth, AccountId, Username, Password) VALUES {string.Join(",", userInserts)}",
                            transaction: transaction);

                        connection.Execute(
                            $"INSERT INTO Account (Id, Name) VALUES {string.Join(",", accountInserts)}",
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
                                    documentInserts.Add($"('{documentNumber}','Document{i}-{batch * batchSize + d}.txt','{documentPath}','{documentLength}','{i}')");
                                }

                                connection.Execute(
                                    $"INSERT INTO Document (Id, Name, FilePath, Length, AccountId) VALUES {string.Join(",", documentInserts)}",
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

                stopwatch.Stop();
                Console.WriteLine($"Data generation completed in {stopwatch.ElapsedMilliseconds / 1000.0} seconds");
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