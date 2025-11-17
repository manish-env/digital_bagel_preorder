import app from './app/index.js';
import { processUploadQueue } from './app/controllers/uploadController.js';

export default {
    ...app,
    async scheduled(event, env, ctx) {
        console.log('Running scheduled task for upload queue processing');
        try {
            await processUploadQueue(env);
            console.log('Upload queue processing completed');
        } catch (error) {
            console.error('Scheduled task failed:', error);
        }
    }
};


