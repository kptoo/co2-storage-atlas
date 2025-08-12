const { Client } = require('pg');
require('dotenv').config();

async function setupDatabase() {
    // First connect to postgres database to create our database
    const adminClient = new Client({
        host: process.env.DB_HOST,
        port: process.env.DB_PORT,
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        database: 'postgres' // Connect to default postgres database
    });

    try {
        await adminClient.connect();
        
        // Create database if it doesn't exist
        try {
            await adminClient.query(`CREATE DATABASE ${process.env.DB_NAME}`);
            console.log(`✅ Database '${process.env.DB_NAME}' created successfully`);
        } catch (error) {
            if (error.code === '42P04') {
                console.log(`ℹ️  Database '${process.env.DB_NAME}' already exists`);
            } else {
                throw error;
            }
        }
        
        await adminClient.end();

        // Now connect to our new database
        const client = new Client({
            host: process.env.DB_HOST,
            port: process.env.DB_PORT,
            user: process.env.DB_USER,
            password: process.env.DB_PASSWORD,
            database: process.env.DB_NAME
        });

        await client.connect();

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

        await client.end();
        console.log('🎉 Database setup completed successfully!');

    } catch (error) {
        console.error('❌ Database setup failed:', error);
        process.exit(1);
    }
}

if (require.main === module) {
    setupDatabase();
}

module.exports = { setupDatabase };