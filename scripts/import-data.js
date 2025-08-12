const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const XLSX = require('xlsx');
const { Client } = require('pg');
require('dotenv').config();

const DATA_DIR = 'C:\\Users\\User\\OneDrive\\Desktop\\Upwork\\neustark\\zurich_data';

async function importData() {
    const client = new Client({
        host: process.env.DB_HOST,
        port: process.env.DB_PORT,
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        database: process.env.DB_NAME
    });

    try {
        await client.connect();
        console.log('🔗 Connected to database');

        // Import CO2 Sources from Excel
        await importCO2Sources(client);
        
        // Import Voting Data
        await importVotingData(client);
        
        // Import Landfills
        await importLandfills(client);
        
        // Import Gravel Pits
        await importGravelPits(client);
        
        // Import Wastewater Plants
        await importWastewaterPlants(client);

        console.log('🎉 Data import completed successfully!');

    } catch (error) {
        console.error('❌ Data import failed:', error);
    } finally {
        await client.end();
    }
}

async function importCO2Sources(client) {
    console.log('📊 Importing CO2 Sources...');
    
    const filePath = path.join(DATA_DIR, 'CO2 sources.xlsx');
    
    try {
        const workbook = XLSX.readFile(filePath);
        const sheetName = workbook.SheetNames[0];
        const sheet = workbook.Sheets[sheetName];
        const data = XLSX.utils.sheet_to_json(sheet);

        for (const row of data) {
            const insertQuery = `
                INSERT INTO co2_sources (
                    plant_name, plant_type, total_co2_t, fossil_co2_t, 
                    geogenic_co2_t, biogenic_co2_t, comment, geom
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, ST_SetSRID(ST_MakePoint($8, $9), 4326))
                ON CONFLICT DO NOTHING
            `;

            await client.query(insertQuery, [
                row['Plant Name'] || '',
                row['Plant Type'] || '',
                parseFloat(row['Total_CO2_t']) || 0,
                parseFloat(row['Fossil_CO2_t']) || 0,
                parseFloat(row['Geogenic_CO2_t']) || 0,
                parseFloat(row['Biogenic_CO2_t']) || 0,
                row['Comment'] || '',
                parseFloat(row['Longitude']) || 0,
                parseFloat(row['Latitude']) || 0
            ]);
        }

        console.log(`✅ Imported ${data.length} CO2 sources`);
    } catch (error) {
        console.error('❌ Error importing CO2 sources:', error);
    }
}

async function importVotingData(client) {
    console.log('🗳️  Importing Voting Data...');
    
    const filePath = path.join(DATA_DIR, 'Voter by communes 2024.csv');
    
    return new Promise((resolve, reject) => {
        const results = [];
        
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', async () => {
                try {
                    for (const row of results) {
                        // Calculate left+green combined percentage
                        const spo_pct = parseFloat(row['SPÖ_percent']) || 0;
                        const grune_pct = parseFloat(row['GRÜNE_percent']) || 0;
                        const kpo_pct = parseFloat(row['KPÖ_percent']) || 0;
                        const left_green_combined = spo_pct + grune_pct + kpo_pct;

                        const insertQuery = `
                            INSERT INTO voting_districts (
                                gkz, name, spo_percent, ovp_percent, fpo_percent,
                                grune_percent, kpo_percent, neos_percent, 
                                left_green_combined, geom
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, ST_SetSRID(ST_MakePoint($10, $11), 4326))
                            ON CONFLICT (gkz) DO NOTHING
                        `;

                        await client.query(insertQuery, [
                            parseInt(row['gkz']) || 0,
                            row['name'] || '',
                            spo_pct,
                            parseFloat(row['ÖVP_percent']) || 0,
                            parseFloat(row['FPÖ_percent']) || 0,
                            grune_pct,
                            kpo_pct,
                            parseFloat(row['NEOS_percent']) || 0,
                            left_green_combined,
                            parseFloat(row['center_lng']) || 0,
                            parseFloat(row['center_lat']) || 0
                        ]);
                    }

                    console.log(`✅ Imported ${results.length} voting districts`);
                    resolve();
                } catch (error) {
                    console.error('❌ Error importing voting data:', error);
                    reject(error);
                }
            });
    });
}

async function importLandfills(client) {
    console.log('🗑️  Importing Landfills...');
    
    const filePath = path.join(DATA_DIR, 'LandfiilsDeponien.csv');
    
    return new Promise((resolve, reject) => {
        const results = [];
        
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', async () => {
                try {
                    for (const row of results) {
                        const insertQuery = `
                            INSERT INTO landfills (
                                company_name, location_name, district, postal_code,
                                address, facility_type, geom
                            ) VALUES ($1, $2, $3, $4, $5, $6, ST_SetSRID(ST_MakePoint($7, $8), 4326))
                        `;

                        await client.query(insertQuery, [
                            row['Firmen_Nam'] || '',
                            row['Standort_N'] || '',
                            row['Standort_B'] || '',
                            parseInt(row['Standort_P']) || null,
                            row['Standort_S'] || '',
                            row['Anlagenbez'] || '',
                            parseFloat(row['Y_Koordina']) || 0, // Note: Y is longitude in this dataset
                            parseFloat(row['X_Koordina']) || 0  // Note: X is latitude in this dataset
                        ]);
                    }

                    console.log(`✅ Imported ${results.length} landfills`);
                    resolve();
                } catch (error) {
                    console.error('❌ Error importing landfills:', error);
                    reject(error);
                }
            });
    });
}

async function importGravelPits(client) {
    console.log('⛏️  Importing Gravel Pits & Stone Quarries...');
    
    const filePath = path.join(DATA_DIR, 'Gravel pits  stone quarries.csv.csv');
    
    return new Promise((resolve, reject) => {
        const results = [];
        
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', async () => {
                try {
                    for (const row of results) {
                        const insertQuery = `
                            INSERT INTO gravel_pits (
                                name, resource, tags, geometry_text, geom
                            ) VALUES ($1, $2, $3, $4, ST_SetSRID(ST_MakePoint($5, $6), 4326))
                        `;

                        await client.query(insertQuery, [
                            row['name'] || '',
                            row['resource'] || '',
                            row['tags'] || '',
                            row['geometry'] || '',
                            parseFloat(row['center_lng']) || 0,
                            parseFloat(row['center_lat']) || 0
                        ]);
                    }

                    console.log(`✅ Imported ${results.length} gravel pits & stone quarries`);
                    resolve();
                } catch (error) {
                    console.error('❌ Error importing gravel pits:', error);
                    reject(error);
                }
            });
    });
}

async function importWastewaterPlants(client) {
    console.log('🏭 Importing Wastewater Treatment Plants...');
    
    const filePath = path.join(DATA_DIR, 'Kläranlagen  Wastewater treatment plants.csv');
    
    return new Promise((resolve, reject) => {
        const results = [];
        
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', async () => {
                try {
                    for (const row of results) {
                        const insertQuery = `
                            INSERT INTO wastewater_plants (
                                pk, pkint, label, begin_date, treatment_type, capacity, geom
                            ) VALUES ($1, $2, $3, $4, $5, $6, ST_SetSRID(ST_MakePoint($7, $8), 4326))
                        `;

                        await client.query(insertQuery, [
                            row['PK'] || '',
                            row['PKINT'] || '',
                            row['LABEL'] || '',
                            row['BEGIN_DAT'] || '',
                            row['ABW_BEHANDLUNG'] || '',
                            parseInt(row['KAPAZITAET']) || null,
                            parseFloat(row['long']) || 0,
                            parseFloat(row['lat']) || 0
                        ]);
                    }

                    console.log(`✅ Imported ${results.length} wastewater treatment plants`);
                    resolve();
                } catch (error) {
                    console.error('❌ Error importing wastewater plants:', error);
                    reject(error);
                }
            });
    });
}

if (require.main === module) {
    importData();
}

module.exports = { importData };