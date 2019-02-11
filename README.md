# world-factbook-indexer
World Factbook Indexer for Cosmos DB / Azure Durable Functions

The CIA World Factbook is a valuable online resource that contains a wealth of information about the countries of the world. It is also public domain, meaning you're free to take the data and use it for your own purposes such as analysis or correlation with other data in your possession. Thanks to Ian Coleman, there is a JSON edition of this data, updated weekly.

World Factbook Indexer is an Azure Durable Function that retrieves this data and updates both a Cosmos DB and Azure Blob Storage.

For details, see this blog post [TODO: URL]

# building

To build this code, you need Visual Studio 2017 with Azure workloads installed.

These cloud assets are needed:
* Azure Function
* Blob storage account
* CosmosDB

You will need to edit Update.cs and supply your own cloud asset names and keys.

# publishing

Download the publishing profile for your Azure Function in the Azure Portal. Right-click the project in Visual Studio and select Publish. The first time you do this, import the publishing profile.



