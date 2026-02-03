import 'dotenv/config';
import AssemblyLotsService from '../services/assemblyLotsService.js';
import { closePool } from '../config/database.js';

const JSON_FILE_PATH = './data/Assembly Lots.json';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 500;

async function assemblyLotsFile() {
    console.log('\n╔════════════════════════════════════════╗');
    console.log('║   Assembly Lots - PostgreSQL Loader    ║');
    console.log('╚════════════════════════════════════════╝\n');

    console.log('📝 Configuration:');
    console.log(`   Database: ${process.env.DB_NAME || 'not set'}`);
    console.log(`   JSON File: ${JSON_FILE_PATH}`);
    console.log(`   Batch Size: ${BATCH_SIZE}\n`);

    try {
        // Process Assembly Lots JSON file
        const result = await AssemblyLotsService.processAssemblyLotsFile(JSON_FILE_PATH, BATCH_SIZE);

        console.log('\n════════════════════════════════════════');
        console.log('            PROCESSING COMPLETE          ');
        console.log('════════════════════════════════════════');
        console.log(`   Total Records:    ${result.total}`);
        console.log(`   Inserted:         ${result.inserted}`);
        console.log(`   Duration:         ${result.duration}s`);
        console.log(`   Records/Second:   ${Math.round(result.inserted / result.duration)}`);
        console.log('════════════════════════════════════════\n');

        // Show total records in database
        const count = await AssemblyLotsService.getCount();
        console.log(`📊 Total records in assembly_mother_lots: ${count}\n`);

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
assemblyLotsFile()
    .then(() => closePool())
    .then(() => process.exit(0))
    .catch((error) => {
        console.error('Fatal error:', error);
        process.exit(1);
    });
