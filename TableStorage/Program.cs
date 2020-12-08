using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TableStorage
{
    public class Employee : TableEntity
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Company { get; set; }
        public string EmployeeId { get; set; }
        public Employee()
        {

        }
        public Employee(string firstName, string lastName, string company, string employeeId)
        {
            FirstName = firstName;
            LastName = lastName;
            Company = PartitionKey = company;
            EmployeeId = RowKey = employeeId;
        }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            var connectionString = @"AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";
            var tableName = "people";
            var employee = new Employee(
                firstName: "Zeeshan",
                lastName: "Meghji",
                company: "Permanent Iteration Inc.",
                employeeId: Guid.NewGuid().ToString()
            );

            //Create table 
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var tableClient = storageAccount.CreateCloudTableClient();
            var table = tableClient.GetTableReference(tableName);
            await table.CreateIfNotExistsAsync();

            //insert record into table
            var insertOperation = TableOperation.InsertOrMerge(employee);
            await table.ExecuteAsync(insertOperation);

            //query for records with specified first name 
            var employeeQuery = new TableQuery<Employee>()
                .Where(TableQuery.GenerateFilterCondition("FirstName", QueryComparisons.Equal, employee.FirstName));
            TableContinuationToken token = null;
            var employees = new List<Employee>();
            do
            {
                var segment = await table.ExecuteQuerySegmentedAsync(employeeQuery, token);
                employees.AddRange(segment.Results.ToList());
            } while (token != null);

            //print results of query
            foreach (var e in employees)
            {
                Console.WriteLine($"{e.FirstName} {e.LastName}");
            }

        }
    }
}
