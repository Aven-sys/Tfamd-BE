import pg from 'pg';
import 'dotenv/config';

const { Pool } = pg;

/**
 * PostgreSQL Connection Pool Configuration
 */
const pool = new Pool({
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT) || 5432,
    database: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    min: parseInt(process.env.DB_POOL_MIN) || 2,
    max: parseInt(process.env.DB_POOL_MAX) || 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
});

// Log pool events
pool.on('connect', () => {
    console.log('📦 New client connected to the pool');
});

pool.on('error', (err) => {
    console.error('❌ Unexpected error on idle client:', err);
    process.exit(-1);
});

/**
 * Get a client from the connection pool
 */
export const getClient = async () => {
    const client = await pool.connect();
    return client;
};

/**
 * Execute a query using the pool
 */
export const query = async (text, params) => {
    const start = Date.now();
    const result = await pool.query(text, params);
    const duration = Date.now() - start;
    console.log(`⚡ Query executed in ${duration}ms | Rows: ${result.rowCount}`);
    return result;
};

/**
 * Close all connections in the pool
 */
export const closePool = async () => {
    await pool.end();
    console.log('🔌 Database pool connections closed');
};

export { pool };
