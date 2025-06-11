import { app, InvocationContext, Timer } from "@azure/functions";
import { CosmosClient } from "@azure/cosmos";

export async function retirehistorydocs(myTimer: Timer, context: InvocationContext): Promise<void> {
    context.log('Timer function processed request.');

    const cosmosConnectionString = process.env["DOCUMENTDB"];
    if (!cosmosConnectionString) {
        context.error("Cosmos DB connection string missing.");
        return;
    }
    const client = new CosmosClient(cosmosConnectionString);
    const database = client.database("chat");
    const container = database.container("history");

    const historyRetentionDays = parseInt(process.env["HISTORY_RETENTION_DAYS"] || "30");
    context.info(`History retention set to ${historyRetentionDays} days.`);

    // Calculate the epoch timestamp
    const now = Math.floor(Date.now() / 1000);
    const retireDate = now - historyRetentionDays * 24 * 60 * 60;

    // 1. Fetch all CHAT_THREAD documents older than the retention period
    const threadQuery = {
        query: "SELECT * FROM c WHERE c.type = 'CHAT_THREAD' AND c._ts < @ts AND (c.isDeleted = false OR IS_NULL(c.isDeleted))",
        parameters: [
            { name: "@ts", value: retireDate }
        ]
    };
    const { resources: oldThreads } = await container.items.query(threadQuery).fetchAll();
    context.log(`Found ${oldThreads.length} CHAT_THREADs older than retention.`);

    for (const thread of oldThreads) {
        // 2. Fetch all documents with chatThreadId or threadId equal to thread.id
        const docsQuery = {
            query: "SELECT * FROM c WHERE (c.chatThreadId = @id OR c.threadId = @id) AND (c.isDeleted = false OR IS_NULL(c.isDeleted))",
            parameters: [
                { name: "@id", value: thread.id }
            ]
        };
        const { resources: relatedDocs } = await container.items.query(docsQuery).fetchAll();
        // 3. Group and check if all are older than retention
        if (relatedDocs.length === 0) {
            context.log(`No related docs for thread id=${thread.id}`);
            thread.isDeleted = true;
            try {
                await container.item(thread.id, thread.userId).replace(thread);
                context.log(`Set isDeleted=true for thread id=${thread.id}`);
            } catch (err) {
                context.error(`Failed to update thread id=${thread.id}: ${err}`);
            }
            continue;
        }
        const allOld = relatedDocs.every(doc => doc._ts < retireDate);
        if (allOld) {
            // 4. Set isDeleted=true for all related docs
            for (const doc of relatedDocs) {
                doc.isDeleted = true;
                try {
                    await container.item(doc.id, doc.userId).replace(doc);
                    context.log(`Set isDeleted=true for document id=${doc.id}`);
                } catch (err) {
                    context.error(`Failed to update document id=${doc.id}: ${err}`);
                }
            }
            // Also set isDeleted=true for the thread itself
            thread.isDeleted = true;
            try {
                await container.item(thread.id, thread.userId).replace(thread);
                context.log(`Set isDeleted=true for thread id=${thread.id}`);
            } catch (err) {
                context.error(`Failed to update thread id=${thread.id}: ${err}`);
            }
        } else {
            context.info(`Not all related docs for thread id=${thread.id} are old enough. Skipping.`);
        }
    }
}

app.timer('retirehistorydocs', {
    schedule: '0 0 2 * * *',
    handler: retirehistorydocs
});
