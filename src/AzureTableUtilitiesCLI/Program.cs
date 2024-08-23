// See https://aka.ms/new-console-template for more information
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using TheByteStuff.AzureTableUtilities;


string source;
string destination;
string name;

if (args.Length > 0)
{
    if (args.Length == 3)
    {
        source = args[0];
        destination = args[1];
        name = args[2];
    }
    else
    {
        Console.WriteLine("Invalid number of arguments");
        return;
    }
}
else
{
    Console.WriteLine("No arguments");
    return;
}

var credentials = new Azure.Identity.DefaultAzureCredential();

var tableClientService = new TableServiceClient(new Uri(source), credentials);
var blobClientService = new BlobServiceClient(new Uri(destination), credentials);

var backup = new BackupAzureTables(tableClientService, blobClientService);

backup.BackupAllTablesToBlob(name, true);