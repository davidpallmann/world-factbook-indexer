# world-factbook-indexer
World Factbook Indexer for Cosmos DB / Azure Durable Functions

The CIA World Factbook is a valuable online resource that contains a wealth of information about the countries of the world. It is also public domain, meaning you're free to take the data and use it for your own purposes such as analysis or correlation with other data in your possession. Thanks to Ian Coleman, there is a JSON edition of this data, updated weekly.

World Factbook Indexer is an Azure Durable Function that retrieves this data and updates both a Cosmos DB and Azure Blob Storage. It creates a back-end repository from which country data can be queries and displayed.

For details, see this [blog post](http://davidpallmann.blogspot.com/2019/02/cia-world-factbook-data-on-azure-part-1.html)

# building

To build this code, you need Visual Studio 2017 with Azure workloads installed.

These cloud assets are needed:
* Azure Function
* Blob storage account
* CosmosDB

You will need to edit Update.cs and supply your own cloud asset names and keys.

# publishing

Download the publishing profile for your Azure Function in the Azure Portal. Right-click the project in Visual Studio and select Publish. The first time you do this, import the publishing profile.



