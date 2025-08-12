const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const path = require('path');
const { Client } = require('pg');
const fs = require('fs');
const csv = require('csv-parser');
const XLSX = require('xlsx');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Database connection
const dbConfig = {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
};

// Auto-setup database function
async function setupDatabaseIfNeeded() {
    const client = new Client(dbConfig);
    
    try {
        await client.connect();
        console.log('🔗 Connected to database');
        
        // Check if tables exist
        const result = await client.query(`
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'co2_sources'
        `);
        
        if (result.rows.length === 0) {
            console.log('📊 Tables not found. Setting up database...');
            
            // Enable PostGIS extension
            await client.query('CREATE EXTENSION IF NOT EXISTS postgis');
            await client.query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"');
            console.log('✅ PostGIS extension enabled');

            // Create tables
            const createTablesSQL = `
                -- Users table for authentication
                CREATE TABLE IF NOT EXISTS users (
                    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    email VARCHAR(255) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    is_admin BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                -- CO2 Sources
                CREATE TABLE IF NOT EXISTS co2_sources (
                    id SERIAL PRIMARY KEY,
                    plant_name VARCHAR(255) NOT NULL,
                    plant_type VARCHAR(100),
                    total_co2_t NUMERIC,
                    fossil_co2_t NUMERIC,
                    geogenic_co2_t NUMERIC,
                    biogenic_co2_t NUMERIC,
                    comment TEXT,
                    geom GEOMETRY(POINT, 4326),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                -- Voting data (semi-transparent choropleth)
                CREATE TABLE IF NOT EXISTS voting_districts (
                    id SERIAL PRIMARY KEY,
                    gkz INTEGER UNIQUE,
                    name VARCHAR(255),
                    spo_percent NUMERIC,
                    ovp_percent NUMERIC,
                    fpo_percent NUMERIC,
                    grune_percent NUMERIC,
                    kpo_percent NUMERIC,
                    neos_percent NUMERIC,
                    left_green_combined NUMERIC, -- SPÖ + GRÜNE + KPÖ
                    geom GEOMETRY(POINT, 4326), -- Using center point for now
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                -- Landfills
                CREATE TABLE IF NOT EXISTS landfills (
                    id SERIAL PRIMARY KEY,
                    company_name VARCHAR(255),
                    location_name VARCHAR(255),
                    district VARCHAR(100),
                    postal_code INTEGER,
                    address VARCHAR(255),
                    facility_type VARCHAR(255),
                    geom GEOMETRY(POINT, 4326),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                -- Gravel pits and stone quarries
                CREATE TABLE IF NOT EXISTS gravel_pits (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    resource VARCHAR(255),
                    tags TEXT,
                    geometry_text TEXT,
                    geom GEOMETRY(POINT, 4326),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                -- Wastewater treatment plants
                CREATE TABLE IF NOT EXISTS wastewater_plants (
                    id SERIAL PRIMARY KEY,
                    pk VARCHAR(50),
                    pkint VARCHAR(50),
                    label VARCHAR(255),
                    begin_date VARCHAR(50),
                    treatment_type VARCHAR(100),
                    capacity INTEGER,
                    geom GEOMETRY(POINT, 4326),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                -- Natural gas pipelines
                CREATE TABLE IF NOT EXISTS gas_pipelines (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    operator VARCHAR(255),
                    diameter INTEGER,
                    pressure_level VARCHAR(50),
                    status VARCHAR(50) DEFAULT 'active',
                    geom GEOMETRY(LINESTRING, 4326),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                -- Natural gas storage sites
                CREATE TABLE IF NOT EXISTS gas_storage_sites (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    operator VARCHAR(255),
                    storage_type VARCHAR(100),
                    capacity_bcm NUMERIC,
                    status VARCHAR(50) DEFAULT 'active',
                    geom GEOMETRY(POINT, 4326),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                -- Research layers (to be populated later)
                -- Highways and major roads
                CREATE TABLE IF NOT EXISTS highways (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    road_type VARCHAR(50),
                    geom GEOMETRY(LINESTRING, 4326),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                -- Railway lines
                CREATE TABLE IF NOT EXISTS railways (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    railway_type VARCHAR(50),
                    geom GEOMETRY(LINESTRING, 4326),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                -- Settlement areas (unsuitable for storage)
                CREATE TABLE IF NOT EXISTS settlement_areas (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    area_type VARCHAR(50),
                    population INTEGER,
                    geom GEOMETRY(POLYGON, 4326),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                -- Nature conservation areas (unsuitable)
                CREATE TABLE IF NOT EXISTS conservation_areas (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    protection_level VARCHAR(100),
                    area_type VARCHAR(100),
                    geom GEOMETRY(POLYGON, 4326),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                -- Groundwater protection areas (unsuitable)
                CREATE TABLE IF NOT EXISTS groundwater_protection (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    protection_zone VARCHAR(50),
                    geom GEOMETRY(POLYGON, 4326),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            `;

            await client.query(createTablesSQL);
            console.log('✅ All tables created successfully');

            // Create spatial indexes
            const spatialIndexes = `
                CREATE INDEX IF NOT EXISTS idx_co2_sources_geom ON co2_sources USING GIST (geom);
                CREATE INDEX IF NOT EXISTS idx_voting_districts_geom ON voting_districts USING GIST (geom);
                CREATE INDEX IF NOT EXISTS idx_landfills_geom ON landfills USING GIST (geom);
                CREATE INDEX IF NOT EXISTS idx_gravel_pits_geom ON gravel_pits USING GIST (geom);
                CREATE INDEX IF NOT EXISTS idx_wastewater_plants_geom ON wastewater_plants USING GIST (geom);
                CREATE INDEX IF NOT EXISTS idx_gas_pipelines_geom ON gas_pipelines USING GIST (geom);
                CREATE INDEX IF NOT EXISTS idx_gas_storage_sites_geom ON gas_storage_sites USING GIST (geom);
                CREATE INDEX IF NOT EXISTS idx_highways_geom ON highways USING GIST (geom);
                CREATE INDEX IF NOT EXISTS idx_railways_geom ON railways USING GIST (geom);
                CREATE INDEX IF NOT EXISTS idx_settlement_areas_geom ON settlement_areas USING GIST (geom);
                CREATE INDEX IF NOT EXISTS idx_conservation_areas_geom ON conservation_areas USING GIST (geom);
                CREATE INDEX IF NOT EXISTS idx_groundwater_protection_geom ON groundwater_protection USING GIST (geom);
            `;

            await client.query(spatialIndexes);
            console.log('✅ Spatial indexes created successfully');

            // Import sample data for demonstration
            await importSampleData(client);
            
            console.log('🎉 Database setup completed successfully!');
        } else {
            console.log('✅ Database tables already exist');
        }
    } catch (error) {
        console.error('❌ Database setup failed:', error);
    } finally {
        await client.end();
    }
}

// Import sample data function
async function importSampleData(client) {
    try {
        console.log('📊 Importing sample data...');
        
        // Sample CO2 sources data
        const sampleCO2Sources = [
            {
                name: "Reststoff-Heizkraftwerk der LINZ AG",
                type: "Waste-to-energy plant",
                total: 250000,
                fossil: 120000,
                biogenic: 130000,
                lat: 48.30639,
                lng: 14.28611,
                comment: "Mixed waste-to-energy plant processing municipal waste and sewage sludge"
            },
            {
                name: "WAV Wels waste incinerator",
                type: "Waste-to-energy plant", 
                total: 343000,
                fossil: 185220,
                biogenic: 157780,
                lat: 48.16667,
                lng: 14.03333,
                comment: "Two-line waste-to-energy plant with household waste processing capacity"
            },
            {
                name: "Reststoffverwertungsanlage Lenzing (RVL)",
                type: "Waste-to-energy plant",
                total: 300000,
                fossil: 180000,
                biogenic: 120000,
                lat: 47.9749,
                lng: 13.6089,
                comment: "Processes sorted waste materials including packaging and wood waste"
            }
        ];

        for (const source of sampleCO2Sources) {
            await client.query(`
                INSERT INTO co2_sources (
                    plant_name, plant_type, total_co2_t, fossil_co2_t, 
                    geogenic_co2_t, biogenic_co2_t, comment, geom
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, ST_SetSRID(ST_MakePoint($8, $9), 4326))
                ON CONFLICT DO NOTHING
            `, [source.name, source.type, source.total, source.fossil, 0, source.biogenic, source.comment, source.lng, source.lat]);
        }

        // Sample voting districts
        const sampleVotingDistricts = [
            { gkz: 40101, name: "Linz", lat: 48.3069, lng: 14.2858, spo: 28.5, grune: 12.3, kpo: 3.2 },
            { gkz: 50101, name: "Salzburg", lat: 47.8095, lng: 13.0550, spo: 24.1, grune: 15.7, kpo: 2.8 },
            { gkz: 40301, name: "Wels", lat: 48.1667, lng: 14.0333, spo: 26.3, grune: 10.5, kpo: 2.1 }
        ];

        for (const district of sampleVotingDistricts) {
            const leftGreen = district.spo + district.grune + district.kpo;
            await client.query(`
                INSERT INTO voting_districts (
                    gkz, name, spo_percent, grune_percent, kpo_percent, 
                    left_green_combined, geom
                ) VALUES ($1, $2, $3, $4, $5, $6, ST_SetSRID(ST_MakePoint($7, $8), 4326))
                ON CONFLICT (gkz) DO NOTHING
            `, [district.gkz, district.name, district.spo, district.grune, district.kpo, leftGreen, district.lng, district.lat]);
        }

        console.log('✅ Sample data imported successfully');
    } catch (error) {
        console.error('❌ Error importing sample data:', error);
    }
}

// Middleware
app.use(helmet({
    contentSecurityPolicy: false // Disable CSP for development
}));

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Rate limiting
const limiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 100 // limit each IP to 100 requests per windowMs
});
app.use('/api/', limiter);

// Serve static files
app.use(express.static(path.join(__dirname, 'public')));

// Routes

// Health check
app.get('/api/health', (req, res) => {
    res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Get all CO2 sources
app.get('/api/co2-sources', async (req, res) => {
    const client = new Client(dbConfig);
    try {
        await client.connect();
        const result = await client.query(`
            SELECT 
                id, plant_name, plant_type, total_co2_t, fossil_co2_t,
                geogenic_co2_t, biogenic_co2_t, comment,
                ST_X(geom) as longitude, ST_Y(geom) as latitude
            FROM co2_sources
            ORDER BY total_co2_t DESC NULLS LAST
        `);
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching CO2 sources:', error);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        await client.end();
    }
});

// Get voting districts (for choropleth)
app.get('/api/voting-districts', async (req, res) => {
    const client = new Client(dbConfig);
    try {
        await client.connect();
        const result = await client.query(`
            SELECT 
                id, gkz, name, spo_percent, ovp_percent, fpo_percent,
                grune_percent, kpo_percent, neos_percent, left_green_combined,
                ST_X(geom) as longitude, ST_Y(geom) as latitude
            FROM voting_districts
            ORDER BY left_green_combined DESC
        `);
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching voting districts:', error);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        await client.end();
    }
});

// Get landfills
app.get('/api/landfills', async (req, res) => {
    const client = new Client(dbConfig);
    try {
        await client.connect();
        const result = await client.query(`
            SELECT 
                id, company_name, location_name, district, postal_code,
                address, facility_type,
                ST_X(geom) as longitude, ST_Y(geom) as latitude
            FROM landfills
            ORDER BY company_name
        `);
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching landfills:', error);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        await client.end();
    }
});

// Get gravel pits
app.get('/api/gravel-pits', async (req, res) => {
    const client = new Client(dbConfig);
    try {
        await client.connect();
        const result = await client.query(`
            SELECT 
                id, name, resource, tags,
                ST_X(geom) as longitude, ST_Y(geom) as latitude
            FROM gravel_pits
            ORDER BY name
        `);
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching gravel pits:', error);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        await client.end();
    }
});

// Get wastewater plants
app.get('/api/wastewater-plants', async (req, res) => {
    const client = new Client(dbConfig);
    try {
        await client.connect();
        const result = await client.query(`
            SELECT 
                id, pk, pkint, label, begin_date, treatment_type, capacity,
                ST_X(geom) as longitude, ST_Y(geom) as latitude
            FROM wastewater_plants
            ORDER BY capacity DESC NULLS LAST
        `);
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching wastewater plants:', error);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        await client.end();
    }
});

// Get gas pipelines
app.get('/api/gas-pipelines', async (req, res) => {
    const client = new Client(dbConfig);
    try {
        await client.connect();
        const result = await client.query(`
            SELECT 
                id, name, operator, diameter, pressure_level, status,
                ST_AsGeoJSON(geom) as geometry
            FROM gas_pipelines
            WHERE status = 'active'
            ORDER BY diameter DESC NULLS LAST
        `);
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching gas pipelines:', error);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        await client.end();
    }
});

// Get gas storage sites
app.get('/api/gas-storage-sites', async (req, res) => {
    const client = new Client(dbConfig);
    try {
        await client.connect();
        const result = await client.query(`
            SELECT 
                id, name, operator, storage_type, capacity_bcm, status,
                ST_X(geom) as longitude, ST_Y(geom) as latitude
            FROM gas_storage_sites
            WHERE status = 'active'
            ORDER BY capacity_bcm DESC NULLS LAST
        `);
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching gas storage sites:', error);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        await client.end();
    }
});

// CRUD operations for admin (to be implemented with authentication)
// For now, basic endpoints without auth

// Create CO2 source
app.post('/api/admin/co2-sources', async (req, res) => {
    const { plant_name, plant_type, total_co2_t, fossil_co2_t, geogenic_co2_t, biogenic_co2_t, comment, longitude, latitude } = req.body;
    
    const client = new Client(dbConfig);
    try {
        await client.connect();
        const result = await client.query(`
            INSERT INTO co2_sources (
                plant_name, plant_type, total_co2_t, fossil_co2_t,
                geogenic_co2_t, biogenic_co2_t, comment, geom
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, ST_SetSRID(ST_MakePoint($8, $9), 4326))
            RETURNING id
        `, [plant_name, plant_type, total_co2_t, fossil_co2_t, geogenic_co2_t, biogenic_co2_t, comment, longitude, latitude]);
        
        res.json({ id: result.rows[0].id, message: 'CO2 source created successfully' });
    } catch (error) {
        console.error('Error creating CO2 source:', error);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        await client.end();
    }
});

// Update CO2 source
app.put('/api/admin/co2-sources/:id', async (req, res) => {
    const { id } = req.params;
    const { plant_name, plant_type, total_co2_t, fossil_co2_t, geogenic_co2_t, biogenic_co2_t, comment, longitude, latitude } = req.body;
    
    const client = new Client(dbConfig);
    try {
        await client.connect();
        await client.query(`
            UPDATE co2_sources SET
                plant_name = $1, plant_type = $2, total_co2_t = $3, fossil_co2_t = $4,
                geogenic_co2_t = $5, biogenic_co2_t = $6, comment = $7,
                geom = ST_SetSRID(ST_MakePoint($8, $9), 4326),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = $10
        `, [plant_name, plant_type, total_co2_t, fossil_co2_t, geogenic_co2_t, biogenic_co2_t, comment, longitude, latitude, id]);
        
        res.json({ message: 'CO2 source updated successfully' });
    } catch (error) {
        console.error('Error updating CO2 source:', error);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        await client.end();
    }
});

// Delete CO2 source
app.delete('/api/admin/co2-sources/:id', async (req, res) => {
    const { id } = req.params;
    
    const client = new Client(dbConfig);
    try {
        await client.connect();
        await client.query('DELETE FROM co2_sources WHERE id = $1', [id]);
        res.json({ message: 'CO2 source deleted successfully' });
    } catch (error) {
        console.error('Error deleting CO2 source:', error);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        await client.end();
    }
});

// CRUD operations for Landfills
app.post('/api/admin/landfills', async (req, res) => {
    const { company_name, location_name, district, address, facility_type, latitude, longitude } = req.body;
    
    const client = new Client(dbConfig);
    try {
        await client.connect();
        const result = await client.query(`
            INSERT INTO landfills (
                company_name, location_name, district, address, facility_type, geom
            ) VALUES ($1, $2, $3, $4, $5, ST_SetSRID(ST_MakePoint($6, $7), 4326))
            RETURNING id
        `, [company_name, location_name, district, address, facility_type, longitude, latitude]);
        
        res.json({ id: result.rows[0].id, message: 'Landfill created successfully' });
    } catch (error) {
        console.error('Error creating landfill:', error);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        await client.end();
    }
});

// CRUD operations for Gravel Pits
app.post('/api/admin/gravel-pits', async (req, res) => {
    const { name, resource, tags, latitude, longitude } = req.body;
    
    const client = new Client(dbConfig);
    try {
        await client.connect();
        const result = await client.query(`
            INSERT INTO gravel_pits (
                name, resource, tags, geom
            ) VALUES ($1, $2, $3, ST_SetSRID(ST_MakePoint($4, $5), 4326))
            RETURNING id
        `, [name, resource, tags, longitude, latitude]);
        
        res.json({ id: result.rows[0].id, message: 'Gravel pit created successfully' });
    } catch (error) {
        console.error('Error creating gravel pit:', error);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        await client.end();
    }
});

// CRUD operations for Wastewater Plants
app.post('/api/admin/wastewater-plants', async (req, res) => {
    const { label, treatment_type, capacity, latitude, longitude } = req.body;
    
    const client = new Client(dbConfig);
    try {
        await client.connect();
        const result = await client.query(`
            INSERT INTO wastewater_plants (
                label, treatment_type, capacity, geom
            ) VALUES ($1, $2, $3, ST_SetSRID(ST_MakePoint($4, $5), 4326))
            RETURNING id
        `, [label, treatment_type, capacity, longitude, latitude]);
        
        res.json({ id: result.rows[0].id, message: 'Wastewater plant created successfully' });
    } catch (error) {
        console.error('Error creating wastewater plant:', error);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        await client.end();
    }
});

// Serve main application
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Error handling middleware
app.use((error, req, res, next) => {
    console.error('Unhandled error:', error);
    res.status(500).json({ error: 'Internal server error' });
});

// Start server with auto-setup
async function startServer() {
    try {
        // Setup database if needed
        await setupDatabaseIfNeeded();
        
        // Start the HTTP server
        app.listen(PORT, () => {
            console.log(`🚀 CO2 Storage Atlas server running on port ${PORT}`);
            console.log(`📍 Access the application at: http://localhost:${PORT}`);
        });
    } catch (error) {
        console.error('❌ Failed to start server:', error);
        process.exit(1);
    }
}

// Initialize the server
startServer();

module.exports = app;