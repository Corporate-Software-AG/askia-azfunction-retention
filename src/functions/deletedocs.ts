import { app, InvocationContext } from "@azure/functions";
import { SearchClient, AzureKeyCredential } from "@azure/search-documents";
import { CosmosClient } from "@azure/cosmos";

export async function deletedocs(documents: unknown[], context: InvocationContext): Promise<void> {
    context.log(`Processing ${documents.length} documents.`);

    // Filter for documents with isDeleted: true and type: 'CHAT_DOCUMENT'
    const deletedChatDocuments = (documents as any[]).filter(doc => doc.isDeleted === true && doc.type === 'CHAT_DOCUMENT');

    for (const doc of deletedChatDocuments) {
        const chatThreadId = doc.chatThreadId;
        context.log(`Deleting all search index docs with chatThreadId: ${chatThreadId}`);

        // Azure Search config from environment
        const searchName = process.env["AZURE_SEARCH_NAME"];
        const searchApiKey = process.env["AZURE_SEARCH_API_KEY"];
        const searchIndex = process.env["AZURE_SEARCH_INDEX"];

        if (!searchName || !searchApiKey || !searchIndex) {
            context.log("Azure Search configuration missing.");
            return;
        }

        // Build the endpoint from the search service name
        const searchEndpoint = `https://${searchName}.search.windows.net`;

        const searchClient = new SearchClient(
            searchEndpoint,
            searchIndex,
            new AzureKeyCredential(searchApiKey)
        );

        // Find all documents in the index with this chatThreadId
        const searchResults = await searchClient.search("*", {
            filter: `chatThreadId eq '${chatThreadId}'`,
            select: ["id"]
        });

        const idsToDelete: { id: string }[] = [];
        for await (const result of searchResults.results) {
            const docAny = result.document as any;
            if (docAny.id) idsToDelete.push({ id: docAny.id });
        }
        if (idsToDelete.length > 0) {
            await searchClient.deleteDocuments(idsToDelete);
            context.log(`Deleted ${idsToDelete.length} documents from search index for chatThreadId: ${chatThreadId}`);
        } else {
            context.log(`No documents found in search index for chatThreadId: ${chatThreadId}`);
        }
    }

    const isDeletedDocuments = (documents as any[]).filter(doc => doc.isDeleted === true);

    // Delete these documents from Cosmos DB
    if (isDeletedDocuments.length > 0) {
        // Get Cosmos DB client
        const cosmosConnectionString = process.env["DOCUMENTDB"];
        if (!cosmosConnectionString) {
            context.log("Cosmos DB connection string missing.");
            return;
        }
        const client = new CosmosClient(cosmosConnectionString);
        const database = client.database("chat");
        const container = database.container("history");
        for (const doc of isDeletedDocuments) {
            try {
                await container.item(doc.id, doc.userId).delete();
                context.log(`Deleted document from Cosmos DB: id=${doc.id}, userId=${doc.userId}`);
            } catch (err) {
                context.log(`Failed to delete document id=${doc.id}: ${err}`);
            }
        }
    }
}

app.cosmosDB('deletedocs', {
    connection: 'DOCUMENTDB',
    databaseName: 'chat',
    containerName: 'history',
    leaseContainerName: 'leases',
    handler: deletedocs
});
