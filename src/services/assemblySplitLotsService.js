import crypto from 'crypto';
import { query, getClient } from '../config/database.js';
import { readJsonFile } from '../utils/fileUtils.js';

/**
 * Assembly Split Lots Service - Inserts assembly split lots data into PostgreSQL
 */
class AssemblySplitLotsService {
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
                    const offset = index * 12;
                    placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12})`);
                    
                    values.push(
                        this.generateUUID(),
                        record.childLotId || '',
                        record.parentLotId || '',
                        record.eqpid || '',
                        record.operationStep || '',
                        record.materialId || '',
                        record.inQty ?? 0,
                        record.outQty ?? 0,
                        record.yieldStatus || '',
                        record.trackInTime || '',
                        record.trackOutTime || '',
                        record.operator || ''
                    );
                });
                
                const insertQuery = `
                    INSERT INTO assembly_split_lots 
                    (id, child_lot_id, parent_lot_id, eqp_id, operation_step, material_id, in_qty, out_qty, yield_status, track_in_time, track_out_time, operator)
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
     * Process JSON file and insert into assembly_split_lots (batch mode)
     */
    static async processAssemblySplitLotsFile(filePath, batchSize = 500) {
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
     * Get total count of records in assembly_split_lots
     */
    static async getCount() {
        const result = await query('SELECT COUNT(*) as total FROM assembly_split_lots');
        return parseInt(result.rows[0].total);
    }
}

export default AssemblySplitLotsService;
