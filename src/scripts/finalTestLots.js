import 'dotenv/config';
import FinalTestLotsService from '../services/finalTestLotsService.js';
import { closePool } from '../config/database.js';

const JSON_FILE_PATH = './data/FT.json';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 500;

async function finalTestLotsFile() {
    console.log('\n╔════════════════════════════════════════╗');
    console.log('║  Final Test Lots - PostgreSQL Loader   ║');
    console.log('╚════════════════════════════════════════╝\n');

    console.log('📝 Configuration:');
    console.log(`   Database: ${process.env.DB_NAME || 'not set'}`);
    console.log(`   JSON File: ${JSON_FILE_PATH}`);
    console.log(`   Batch Size: ${BATCH_SIZE}\n`);

    try {
        // Process Final Test Lots JSON file
        const result = await FinalTestLotsService.processFinalTestLotsFile(JSON_FILE_PATH, BATCH_SIZE);

        console.log('\n════════════════════════════════════════');
        console.log('            PROCESSING COMPLETE          ');
        console.log('════════════════════════════════════════');
        console.log(`   Total Records:    ${result.total}`);
        console.log(`   Inserted:         ${result.inserted}`);
        console.log(`   Duration:         ${result.duration}s`);
        if (result.inserted > 0 && result.duration > 0) {
            console.log(`   Records/Second:   ${Math.round(result.inserted / result.duration)}`);
        }
        console.log('════════════════════════════════════════\n');

        // Show total records in database
        const count = await FinalTestLotsService.getCount();
        console.log(`📊 Total records in final_test_lots: ${count}\n`);

    } catch (error) {
        console.error('❌ Error:', error.message);
        process.exit(1);
    }
}

// Graceful shutdown
process.on('SIGINT', async () => {
    await closePool();
    process.exit(0);
});

// Run application
finalTestLotsFile()
    .then(() => closePool())
    .then(() => process.exit(0))
    .catch((error) => {
        console.error('Fatal error:', error);
        process.exit(1);
    });
