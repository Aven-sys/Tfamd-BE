import crypto from 'crypto';
import { query, getClient } from '../config/database.js';
import { readJsonFile } from '../utils/fileUtils.js';

/**
 * Final Test Lots Service - Inserts final test lots data into PostgreSQL
 */
class FinalTestLotsService {
    /**
     * Generate a unique UUID
     */
    static generateUUID() {
        return crypto.randomUUID();
    }

    /**
     * Insert multiple records using batch insert with transaction
     */
    static async insertMany(records, batchSize = 500) {
        const client = await getClient();
        let insertedCount = 0;
        const startTime = Date.now();
        
        try {
            await client.query('BEGIN');
            
            const totalBatches = Math.ceil(records.length / batchSize);
            
            for (let i = 0; i < records.length; i += batchSize) {
                const batch = records.slice(i, i + batchSize);
                const values = [];
                const placeholders = [];
                
                batch.forEach((record, index) => {
                    const offset = index * 20;
                    placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12}, $${offset + 13}, $${offset + 14}, $${offset + 15}, $${offset + 16}, $${offset + 17}, $${offset + 18}, $${offset + 19}, $${offset + 20})`);
                    
                    values.push(
                        this.generateUUID(),
                        record.ftLotId || '',
                        record.assyMotherLot || '',
                        record.device || '',
                        record.vendorLot || '',
                        record.testEqpId || '',
                        record.inQty ?? 0,
                        record.outQty ?? 0,
                        record.yield ?? 0,
                        record.fpy ?? 0,
                        record.bin || '',
                        record.openRejects ?? 0,
                        record.shortRejects ?? 0,
                        record.funcRejects ?? 0,
                        record.status || '',
                        record.operator || '',
                        record.startTime || '',
                        record.endTime || '',
                        record.customer || '',
                        record.plant || ''
                    );
                });
                
                const insertQuery = `
                    INSERT INTO final_test_lots 
                    (id, ft_lot_id, assy_mother_lot, device, vendor_lot, test_eqp_id, in_qty, out_qty, yield, fpy, bin, open_rejects, short_rejects, func_rejects, status, operator, start_time, end_time, customer, plant)
                    VALUES ${placeholders.join(', ')}
                `;
                
                const result = await client.query(insertQuery, values);
                insertedCount += result.rowCount;
                
                const batchNum = Math.floor(i / batchSize) + 1;
                const percent = Math.round((batchNum / totalBatches) * 100);
                console.log(`📥 Batch ${batchNum}/${totalBatches} (${percent}%): Inserted ${result.rowCount} records`);
            }
            
            await client.query('COMMIT');
            const duration = ((Date.now() - startTime) / 1000).toFixed(2);
            console.log(`✅ Transaction committed in ${duration}s`);
            
            return { total: records.length, inserted: insertedCount, duration: parseFloat(duration) };
        } catch (error) {
            await client.query('ROLLBACK');
            console.error('❌ Transaction rolled back:', error.message);
            throw error;
        } finally {
            client.release();
            console.log('🔄 Client released back to pool');
        }
    }

    /**
     * Process JSON file and insert into final_test_lots (batch mode)
     */
    static async processFinalTestLotsFile(filePath, batchSize = 500) {
        // Check if records already exist in database
        const existingCount = await this.getCount();
        if (existingCount > 5) {
            console.log(`ℹ️ Database already has ${existingCount} records. Skipping insert.`);
            return { total: 0, inserted: 0, skipped: true, duration: 0 };
        }

        const jsonData = await readJsonFile(filePath);
        return await this.insertMany(jsonData, batchSize);
    }

    /**
     * Get total count of records in final_test_lots
     */
    static async getCount() {
        const result = await query('SELECT COUNT(*) as total FROM final_test_lots');
        return parseInt(result.rows[0].total);
    }
}

export default FinalTestLotsService;
