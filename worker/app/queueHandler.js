import { processUploadInBackground } from './controllers/uploadController.js';

export async function handleQueueMessage(batch, env) {
    console.log('Queue message received:', batch.messages.length, 'messages');

    for (const message of batch.messages) {
        try {
            const { uploadId, rows, namespace } = message.body;

            console.log(`Processing upload ${uploadId} with ${rows.length} rows`);

            // Process the upload
            await processUploadInBackground(env, uploadId, rows, namespace);

            console.log(`Upload ${uploadId} processing completed`);

            // Mark message as processed
            message.ack();

        } catch (error) {
            console.error('Error processing queue message:', error);
            // Mark message as failed - it will be retried
            message.retry();
        }
    }
}

