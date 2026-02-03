import fs from 'fs';
import path from 'path';

/**
 * Read and parse a JSON file
 * @param {string} filePath - Path to the JSON file
 * @returns {Promise<Array>} Parsed JSON array
 */
export const readJsonFile = async (filePath) => {
    const absolutePath = path.resolve(filePath);
    console.log(`📂 Reading JSON file: ${absolutePath}`);
    
    if (!fs.existsSync(absolutePath)) {
        throw new Error(`File not found: ${absolutePath}`);
    }

    const stats = fs.statSync(absolutePath);
    const fileSizeMB = (stats.size / (1024 * 1024)).toFixed(2);
    console.log(`📊 File size: ${fileSizeMB} MB`);
    
    const data = fs.readFileSync(absolutePath, 'utf8');
    const jsonData = JSON.parse(data);
    
    if (!Array.isArray(jsonData)) {
        throw new Error('JSON file must contain an array');
    }
    
    console.log(`✅ Parsed ${jsonData.length} objects from JSON file`);
    return jsonData;
};
