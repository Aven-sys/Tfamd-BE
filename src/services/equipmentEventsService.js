import crypto from 'crypto';
import { query, getClient } from '../config/database.js';
import { readJsonFile } from '../utils/fileUtils.js';

/**
 * Equipment Events Service - Inserts equipment events data into PostgreSQL
 */
class EquipmentEventsService {
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
                    const offset = index * 11;
                    placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11})`);
                    
                    // Format affected_lots as PostgreSQL array
                    let affectedLots = record.affectedLots;
                    if (affectedLots && typeof affectedLots === 'string') {
                        affectedLots = `{${affectedLots}}`;
                    } else if (Array.isArray(affectedLots)) {
                        affectedLots = `{${affectedLots.join(',')}}`;
                    } else {
                        affectedLots = '{}';
                    }
                    
                    values.push(
                        this.generateUUID(),
                        record.eventId?.toString() || '',
                        record.eqpId || '',
                        record.alarmCode?.toString() || '',
                        record.alarmDesc || '',
                        record.alarmTs || null,
                        record.severity || '',
                        record.resolutionTime || null,
                        affectedLots,
                        record.operator || '',
                        record.notes || ''
                    );
                });
                
                const insertQuery = `
                    INSERT INTO equipment_events 
                    (id, event_id, eqp_id, alarm_code, alarm_desc, alarm_ts, severity, resolution_time, affected_lots, operator, notes)
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
     * Process JSON file and insert into equipment_events (batch mode)
     */
    static async processEquipmentEventsFile(filePath, batchSize = 500) {
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
     * Get total count of records in equipment_events
     */
    static async getCount() {
        const result = await query('SELECT COUNT(*) as total FROM equipment_events');
        return parseInt(result.rows[0].total);
    }
}

export default EquipmentEventsService;
