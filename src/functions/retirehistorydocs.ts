import { app, InvocationContext, Timer } from "@azure/functions";
import { CosmosClient } from "@azure/cosmos";

export async function retirehistorydocs(myTimer: Timer, context: InvocationContext): Promise<void> {
    context.log('Timer function processed request.');

    const cosmosConnectionString = process.env["DOCUMENTDB"];
    if (!cosmosConnectionString) {
        context.log("Cosmos DB connection string missing.");
        return;
    }
    const client = new CosmosClient(cosmosConnectionString);
    const database = client.database("chat");
    const container = database.container("history");

    const historyRetentionDays = parseInt(process.env["HISTORY_RETENTION_DAYS"] || "30");

    // Calculate the epoch timestamp
    const now = Math.floor(Date.now() / 1000);
    const retireDate = now - historyRetentionDays * 24 * 60 * 60;

    // Query for documents with _ts < retireDate and (isDeleted = false or isDeleted is null)
    const query = {
        query: "SELECT * FROM c WHERE c._ts < @ts AND (c.isDeleted = false OR IS_NULL(c.isDeleted))",
        parameters: [
            { name: "@ts", value: retireDate }
        ]
    };

    const { resources: oldDocs } = await container.items.query(query).fetchAll();
    context.log(`Found ${oldDocs.length} documents not modified in the last 30 days.`);

    for (const doc of oldDocs) {
        doc.isDeleted = true;
        try {
            await container.item(doc.id, doc.userId).replace(doc);
            context.log(`Set isDeleted=true for document id=${doc.id}`);
        } catch (err) {
            context.log(`Failed to update document id=${doc.id}: ${err}`);
        }
    }
}

app.timer('retirehistorydocs', {
    schedule: '0 0 2 * * *',
    handler: retirehistorydocs
});
