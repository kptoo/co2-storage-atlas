const { Client } = require('pg');
require('dotenv').config();

class DatabaseSetup {
    constructor() {
        this.adminClient = new Client({
            host: process.env.DB_HOST || 'localhost',
            port: process.env.DB_PORT || 5432,
            user: process.env.DB_USER || 'postgres',
            password: process.env.DB_PASSWORD,
            database: 'postgres' // Connect to default database first
        });
        
        this.dbClient = null;
    }

    async setup() {
        try {
            console.log('🚀 Starting database setup...\n');
            
            // Connect to admin database
            await this.adminClient.connect();
            console.log('✅ Connected to PostgreSQL server');
            
            // Create database if it doesn't exist
            await this.createDatabase();
            
            // Reconnect to the new database
            await this.adminClient.end();
            this.dbClient = new Client({
                host: process.env.DB_HOST || 'localhost',
                port: process.env.DB_PORT || 5432,
                user: process.env.DB_USER || 'postgres',
                password: process.env.DB_PASSWORD,
                database: process.env.DB_NAME || 'co2_storage_atlas'
            });
            await this.dbClient.connect();
            console.log('✅ Connected to project database');
            
            // Setup PostGIS with EPSG:4326 focus
            await this.setupPostGIS();
            
            // Create all tables with proper SRID
            await this.createTables();
            
            // Create spatial indexes
            await this.createIndexes();
            
            // Insert default configuration data
            await this.insertDefaultData();
            
            console.log('\n🎉 Database setup completed successfully!');
            console.log('📊 Database is ready for data import');
            
        } catch (error) {
            console.error('❌ Database setup failed:', error);
            process.exit(1);
        } finally {
            if (this.adminClient && !this.adminClient._ending) {
                await this.adminClient.end();
            }
            if (this.dbClient && !this.dbClient._ending) {
                await this.dbClient.end();
            }
        }
    }

    async createDatabase() {
        const dbName = process.env.DB_NAME || 'co2_storage_atlas';
        
        try {
            // Check if database exists
            const checkQuery = `SELECT 1 FROM pg_database WHERE datname = $1`;
            const result = await this.adminClient.query(checkQuery, [dbName]);
            
            if (result.rows.length === 0) {
                await this.adminClient.query(`CREATE DATABASE ${dbName}`);
                console.log(`✅ Database '${dbName}' created`);
            } else {
                console.log(`✅ Database '${dbName}' already exists`);
            }
        } catch (error) {
            console.error('❌ Error creating database:', error);
            throw error;
        }
    }

    async setupPostGIS() {
        try {
            // Enable PostGIS extension
            await this.dbClient.query('CREATE EXTENSION IF NOT EXISTS postgis');
            console.log('✅ PostGIS extension enabled');
            
            // Enable PostGIS topology if available
            try {
                await this.dbClient.query('CREATE EXTENSION IF NOT EXISTS postgis_topology');
                console.log('✅ PostGIS Topology extension enabled');
            } catch (topologyError) {
                console.log('⚠️  PostGIS Topology extension not available (not required)');
            }
            
            // Verify EPSG:4326 is available
            const sridCheck = await this.dbClient.query(`
                SELECT srid, proj4text FROM spatial_ref_sys WHERE srid = 4326
            `);
            
            if (sridCheck.rows.length > 0) {
                console.log('✅ EPSG:4326 (WGS84) coordinate system verified');
            } else {
                console.error('❌ EPSG:4326 not found in spatial_ref_sys');
                throw new Error('Required coordinate system EPSG:4326 not available');
            }
            
        } catch (error) {
            console.error('❌ Error setting up PostGIS:', error);
            throw error;
        }
    }

    async createTables() {
        const queries = [
            // Study area boundaries (EPSG:4326)
            `CREATE TABLE IF NOT EXISTS study_area_boundaries (
                id SERIAL PRIMARY KEY,
                g_id VARCHAR(50) UNIQUE,
                g_name VARCHAR(255),
                state VARCHAR(100),
                geom GEOMETRY(MULTIPOLYGON, 4326),
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // CO2 Sources with enhanced columns for performance
            `CREATE TABLE IF NOT EXISTS co2_sources (
                id SERIAL PRIMARY KEY,
                plant_name VARCHAR(255) NOT NULL,
                plant_type VARCHAR(100),
                total_co2_t NUMERIC(12,2) DEFAULT 0,
                fossil_co2_t NUMERIC(12,2) DEFAULT 0,
                biogenic_co2_t NUMERIC(12,2) DEFAULT 0,
                comment TEXT,
                is_prominent BOOLEAN DEFAULT FALSE,
                pin_size INTEGER DEFAULT 2,
                pin_color VARCHAR(7) DEFAULT '#ff4444',
                icon_url VARCHAR(500),
                opacity NUMERIC(3,2) DEFAULT 1.0,
                geom GEOMETRY(POINT, 4326),
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Voting Districts with enhanced geometry handling
            `CREATE TABLE IF NOT EXISTS voting_districts (
                id SERIAL PRIMARY KEY,
                gkz INTEGER UNIQUE,
                name VARCHAR(255),
                spo_percent NUMERIC(5,2) DEFAULT 0,
                ovp_percent NUMERIC(5,2) DEFAULT 0,
                fpo_percent NUMERIC(5,2) DEFAULT 0,
                grune_percent NUMERIC(5,2) DEFAULT 0,
                kpo_percent NUMERIC(5,2) DEFAULT 0,
                neos_percent NUMERIC(5,2) DEFAULT 0,
                left_green_combined NUMERIC(5,2) DEFAULT 0,
                choropleth_color VARCHAR(7),
                geom GEOMETRY(MULTIPOLYGON, 4326),
                center_point GEOMETRY(POINT, 4326),
                properties JSONB,
                geometry_valid BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Landfills with icon support
            `CREATE TABLE IF NOT EXISTS landfills (
                id SERIAL PRIMARY KEY,
                company_name VARCHAR(255),
                location_name VARCHAR(255),
                district VARCHAR(100),
                address VARCHAR(500),
                facility_type VARCHAR(255),
                pin_size INTEGER DEFAULT 2,
                pin_color VARCHAR(7) DEFAULT '#ff8800',
                icon_url VARCHAR(500),
                opacity NUMERIC(3,2) DEFAULT 0.8,
                geom GEOMETRY(POINT, 4326),
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Gravel Pits with icon support
            `CREATE TABLE IF NOT EXISTS gravel_pits (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                resource VARCHAR(255),
                tags TEXT,
                pin_size INTEGER DEFAULT 2,
                pin_color VARCHAR(7) DEFAULT '#8855aa',
                icon_url VARCHAR(500),
                opacity NUMERIC(3,2) DEFAULT 0.7,
                geom GEOMETRY(POINT, 4326),
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Wastewater Plants with icon support
            `CREATE TABLE IF NOT EXISTS wastewater_plants (
                id SERIAL PRIMARY KEY,
                pk VARCHAR(50),
                label VARCHAR(255),
                treatment_type VARCHAR(100),
                capacity INTEGER,
                pin_size INTEGER DEFAULT 2,
                pin_color VARCHAR(7) DEFAULT '#3388ff',
                icon_url VARCHAR(500),
                opacity NUMERIC(3,2) DEFAULT 0.6,
                geom GEOMETRY(POINT, 4326),
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Gas Pipelines with enhanced performance columns
            `CREATE TABLE IF NOT EXISTS gas_pipelines (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                operator VARCHAR(255),
                diameter INTEGER,
                pressure_level VARCHAR(50),
                pipeline_type VARCHAR(100),
                line_color VARCHAR(7) DEFAULT '#00aa44',
                line_weight INTEGER DEFAULT 4,
                line_opacity NUMERIC(3,2) DEFAULT 0.8,
                geom GEOMETRY(MULTILINESTRING, 4326),
                simplified_geom GEOMETRY(MULTILINESTRING, 4326),
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Gas Storage Sites with icon support
            `CREATE TABLE IF NOT EXISTS gas_storage_sites (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                operator VARCHAR(255),
                storage_type VARCHAR(100),
                capacity_bcm NUMERIC(10,3),
                pin_size INTEGER DEFAULT 2,
                pin_color VARCHAR(7) DEFAULT '#00cc88',
                icon_url VARCHAR(500),
                opacity NUMERIC(3,2) DEFAULT 0.5,
                geom GEOMETRY(POINT, 4326),
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Gas Distribution Points with icon support
            `CREATE TABLE IF NOT EXISTS gas_distribution_points (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                type VARCHAR(100),
                operator VARCHAR(255),
                pin_size INTEGER DEFAULT 1,
                pin_color VARCHAR(7) DEFAULT '#00aa44',
                icon_url VARCHAR(500),
                opacity NUMERIC(3,2) DEFAULT 0.4,
                geom GEOMETRY(POINT, 4326),
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Compressor Stations with icon support
            `CREATE TABLE IF NOT EXISTS compressor_stations (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                operator VARCHAR(255),
                capacity_info TEXT,
                pin_size INTEGER DEFAULT 2,
                pin_color VARCHAR(7) DEFAULT '#ffaa00',
                icon_url VARCHAR(500),
                opacity NUMERIC(3,2) DEFAULT 0.3,
                geom GEOMETRY(POINT, 4326),
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Groundwater Protection with simplified geometries
            `CREATE TABLE IF NOT EXISTS groundwater_protection (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                protection_zone VARCHAR(50),
                fill_color VARCHAR(7) DEFAULT '#0066ff',
                fill_opacity NUMERIC(3,2) DEFAULT 0.3,
                border_color VARCHAR(7) DEFAULT '#0044cc',
                border_weight INTEGER DEFAULT 2,
                geom GEOMETRY(MULTIPOLYGON, 4326),
                simplified_geom GEOMETRY(MULTIPOLYGON, 4326),
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Conservation Areas with simplified geometries
            `CREATE TABLE IF NOT EXISTS conservation_areas (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                protection_level VARCHAR(100),
                area_type VARCHAR(100),
                fill_color VARCHAR(7) DEFAULT '#00ff00',
                fill_opacity NUMERIC(3,2) DEFAULT 0.3,
                border_color VARCHAR(7) DEFAULT '#00cc00',
                border_weight INTEGER DEFAULT 2,
                geom GEOMETRY(MULTIPOLYGON, 4326),
                simplified_geom GEOMETRY(MULTIPOLYGON, 4326),
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Settlement Areas with simplified geometries
            `CREATE TABLE IF NOT EXISTS settlement_areas (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                area_type VARCHAR(50),
                population INTEGER,
                fill_color VARCHAR(7) DEFAULT '#ff0000',
                fill_opacity NUMERIC(3,2) DEFAULT 0.3,
                border_color VARCHAR(7) DEFAULT '#cc0000',
                border_weight INTEGER DEFAULT 2,
                geom GEOMETRY(MULTIPOLYGON, 4326),
                simplified_geom GEOMETRY(MULTIPOLYGON, 4326),
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Highways with simplified geometries
            `CREATE TABLE IF NOT EXISTS highways (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                highway_number VARCHAR(50),
                road_type VARCHAR(50),
                line_color VARCHAR(7) DEFAULT '#666666',
                line_weight INTEGER DEFAULT 3,
                line_opacity NUMERIC(3,2) DEFAULT 0.7,
                geom GEOMETRY(MULTILINESTRING, 4326),
                simplified_geom GEOMETRY(MULTILINESTRING, 4326),
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Railways with simplified geometries
            `CREATE TABLE IF NOT EXISTS railways (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                railway_type VARCHAR(50),
                operator VARCHAR(255),
                line_color VARCHAR(7) DEFAULT '#8B4513',
                line_weight INTEGER DEFAULT 3,
                line_opacity NUMERIC(3,2) DEFAULT 0.8,
                geom GEOMETRY(MULTILINESTRING, 4326),
                simplified_geom GEOMETRY(MULTILINESTRING, 4326),
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Layer Styles Configuration
            `CREATE TABLE IF NOT EXISTS layer_styles (
                id SERIAL PRIMARY KEY,
                layer_name VARCHAR(100) UNIQUE,
                style_config JSONB,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Admin Users table
            `CREATE TABLE IF NOT EXISTS admin_users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                email VARCHAR(255) UNIQUE,
                password_hash VARCHAR(255) NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                last_login TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Audit Log for changes
            `CREATE TABLE IF NOT EXISTS audit_log (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(100),
                record_id INTEGER,
                action VARCHAR(10), -- INSERT, UPDATE, DELETE
                old_values JSONB,
                new_values JSONB,
                user_id INTEGER REFERENCES admin_users(id),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`
        ];

        console.log('📋 Creating database tables...');
        for (let i = 0; i < queries.length; i++) {
            try {
                await this.dbClient.query(queries[i]);
                const tableName = queries[i].match(/CREATE TABLE IF NOT EXISTS (\w+)/)[1];
                console.log(`✅ Table '${tableName}' created/verified`);
            } catch (error) {
                console.error(`❌ Error creating table ${i + 1}:`, error);
                throw error;
            }
        }
    }

    async createIndexes() {
        const indexes = [
            // Spatial indexes for EPSG:4326 geometries
            'CREATE INDEX IF NOT EXISTS idx_study_area_geom ON study_area_boundaries USING GIST (geom)',
            'CREATE INDEX IF NOT EXISTS idx_co2_sources_geom ON co2_sources USING GIST (geom)',
            'CREATE INDEX IF NOT EXISTS idx_co2_sources_prominent ON co2_sources(is_prominent, total_co2_t DESC)',
            'CREATE INDEX IF NOT EXISTS idx_voting_districts_geom ON voting_districts USING GIST (geom)',
            'CREATE INDEX IF NOT EXISTS idx_voting_districts_center ON voting_districts USING GIST (center_point)',
            'CREATE INDEX IF NOT EXISTS idx_voting_districts_gkz ON voting_districts(gkz)',
            'CREATE INDEX IF NOT EXISTS idx_voting_districts_valid ON voting_districts(geometry_valid)',
            'CREATE INDEX IF NOT EXISTS idx_landfills_geom ON landfills USING GIST (geom)',
            'CREATE INDEX IF NOT EXISTS idx_gravel_pits_geom ON gravel_pits USING GIST (geom)',
            'CREATE INDEX IF NOT EXISTS idx_wastewater_plants_geom ON wastewater_plants USING GIST (geom)',
            'CREATE INDEX IF NOT EXISTS idx_gas_pipelines_geom ON gas_pipelines USING GIST (geom)',
            'CREATE INDEX IF NOT EXISTS idx_gas_pipelines_simplified ON gas_pipelines USING GIST (simplified_geom)',
            'CREATE INDEX IF NOT EXISTS idx_gas_storage_sites_geom ON gas_storage_sites USING GIST (geom)',
            'CREATE INDEX IF NOT EXISTS idx_gas_distribution_points_geom ON gas_distribution_points USING GIST (geom)',
            'CREATE INDEX IF NOT EXISTS idx_compressor_stations_geom ON compressor_stations USING GIST (geom)',
            'CREATE INDEX IF NOT EXISTS idx_groundwater_protection_geom ON groundwater_protection USING GIST (geom)',
            'CREATE INDEX IF NOT EXISTS idx_groundwater_simplified ON groundwater_protection USING GIST (simplified_geom)',
            'CREATE INDEX IF NOT EXISTS idx_conservation_areas_geom ON conservation_areas USING GIST (geom)',
            'CREATE INDEX IF NOT EXISTS idx_conservation_simplified ON conservation_areas USING GIST (simplified_geom)',
            'CREATE INDEX IF NOT EXISTS idx_settlement_areas_geom ON settlement_areas USING GIST (geom)',
            'CREATE INDEX IF NOT EXISTS idx_settlement_simplified ON settlement_areas USING GIST (simplified_geom)',
            'CREATE INDEX IF NOT EXISTS idx_highways_geom ON highways USING GIST (geom)',
            'CREATE INDEX IF NOT EXISTS idx_highways_simplified ON highways USING GIST (simplified_geom)',
            'CREATE INDEX IF NOT EXISTS idx_railways_geom ON railways USING GIST (geom)',
            'CREATE INDEX IF NOT EXISTS idx_railways_simplified ON railways USING GIST (simplified_geom)',
            
            // Performance indexes for frequent queries
            'CREATE INDEX IF NOT EXISTS idx_co2_sources_type ON co2_sources(plant_type)',
            'CREATE INDEX IF NOT EXISTS idx_voting_left_green ON voting_districts(left_green_combined DESC)',
            'CREATE INDEX IF NOT EXISTS idx_layer_styles_name ON layer_styles(layer_name)',
            'CREATE INDEX IF NOT EXISTS idx_layer_styles_active ON layer_styles(is_active)',
            'CREATE INDEX IF NOT EXISTS idx_admin_users_username ON admin_users(username)',
            'CREATE INDEX IF NOT EXISTS idx_admin_users_email ON admin_users(email)',
            'CREATE INDEX IF NOT EXISTS idx_audit_log_table ON audit_log(table_name, record_id)',
            'CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log(timestamp DESC)',
            
            // Composite indexes for common queries
            'CREATE INDEX IF NOT EXISTS idx_co2_sources_type_prominent ON co2_sources(plant_type, is_prominent, total_co2_t DESC)',
            'CREATE INDEX IF NOT EXISTS idx_voting_name_gkz ON voting_districts(name, gkz)'
        ];

        console.log('📊 Creating spatial and performance indexes...');
        for (const index of indexes) {
            try {
                await this.dbClient.query(index);
                const indexName = index.match(/CREATE INDEX IF NOT EXISTS (\w+)/)[1];
                console.log(`✅ Index '${indexName}' created/verified`);
            } catch (error) {
                console.error(`❌ Error creating index:`, error.message);
                // Continue with other indexes
            }
        }
    }

    async insertDefaultData() {
        console.log('📝 Inserting default configuration data...');
        
        // Insert default layer styles with icon configurations
        const styles = [
            { 
                name: 'co2_sources', 
                config: { 
                    default_color: '#ff4444', 
                    prominent_color: '#cc0000', 
                    prominent_size: 16, 
                    normal_size: 12, 
                    opacity: 0.9,
                    pulse_animation: true,
                    icon_path: '/icons/CO₂ Sources.png',
                    cluster_enabled: true
                } 
            },
            { 
                name: 'voting_districts', 
                config: { 
                    opacity: 0.7, 
                    border_color: '#ffffff', 
                    border_weight: 2, 
                    choropleth: true 
                } 
            },
            { 
                name: 'landfills', 
                config: { 
                    default_color: '#ff8800', 
                    size: 10, 
                    opacity: 0.8,
                    hover_effect: true,
                    icon_path: '/icons/Landfills.png',
                    cluster_enabled: true
                } 
            },
            { 
                name: 'gravel_pits', 
                config: { 
                    default_color: '#8855aa', 
                    size: 8, 
                    opacity: 0.7,
                    cluster: true,
                    icon_path: '/icons/Gravel Pits.png',
                    cluster_enabled: true
                } 
            },
            { 
                name: 'wastewater_plants', 
                config: { 
                    default_color: '#3388ff', 
                    size: 10, 
                    opacity: 0.6,
                    capacity_scaling: true,
                    icon_path: '/icons/Wastewater Plants.png',
                    cluster_enabled: true
                } 
            },
            { 
                name: 'gas_pipelines', 
                config: { 
                    default_color: '#00aa44', 
                    weight: 4, 
                    opacity: 0.8, 
                    dash_array: null 
                } 
            },
            { 
                name: 'gas_storage_sites', 
                config: { 
                    default_color: '#00cc88', 
                    size: 12, 
                    opacity: 0.5,
                    capacity_scaling: true,
                    icon_path: '/icons/Gas Storage.png',
                    cluster_enabled: true
                } 
            },
            { 
                name: 'gas_distribution_points', 
                config: { 
                    default_color: '#00aa44', 
                    size: 6, 
                    opacity: 0.4,
                    cluster: true,
                    icon_path: '/icons/Gas Distribution.png',
                    cluster_enabled: true
                } 
            },
            { 
                name: 'compressor_stations', 
                config: { 
                    default_color: '#ffaa00', 
                    size: 10, 
                    opacity: 0.3,
                    hover_effect: true,
                    icon_path: '/icons/Compressor Stations.png',
                    cluster_enabled: true
                } 
            },
            { 
                name: 'groundwater_protection', 
                config: { 
                    fill_color: '#0066ff', 
                    fill_opacity: 0.3, 
                    border_color: '#0044cc' 
                } 
            },
            { 
                name: 'conservation_areas', 
                config: { 
                    fill_color: '#00ff00', 
                    fill_opacity: 0.3, 
                    border_color: '#00cc00' 
                } 
            },
            { 
                name: 'settlement_areas', 
                config: { 
                    fill_color: '#ff0000', 
                    fill_opacity: 0.3, 
                    border_color: '#cc0000' 
                } 
            },
            { 
                name: 'highways', 
                config: { 
                    default_color: '#666666', 
                    weight: 3, 
                    opacity: 0.7 
                } 
            },
            { 
                name: 'railways', 
                config: { 
                    default_color: '#8B4513', 
                    weight: 3, 
                    opacity: 0.8, 
                    dash_array: '10,5' 
                } 
            }
        ];

        for (const style of styles) {
            try {
                await this.dbClient.query(
                    `INSERT INTO layer_styles (layer_name, style_config) 
                     VALUES ($1, $2) 
                     ON CONFLICT (layer_name) DO UPDATE SET 
                        style_config = EXCLUDED.style_config,
                        updated_at = CURRENT_TIMESTAMP`,
                    [style.name, JSON.stringify(style.config)]
                );
                console.log(`✅ Style config for '${style.name}' inserted/updated`);
            } catch (error) {
                console.error(`❌ Error inserting style for ${style.name}:`, error.message);
            }
        }

        // Create default admin user
        try {
            const bcrypt = require('bcryptjs');
            const passwordHash = await bcrypt.hash(process.env.ADMIN_PASSWORD || 'co2atlas2024', 12);
            
            await this.dbClient.query(
                `INSERT INTO admin_users (username, email, password_hash) 
                 VALUES ($1, $2, $3) 
                 ON CONFLICT (username) DO UPDATE SET 
                    password_hash = EXCLUDED.password_hash,
                    updated_at = CURRENT_TIMESTAMP`,
                ['admin', process.env.ADMIN_EMAIL || 'admin@co2atlas.local', passwordHash]
            );
            console.log('✅ Default admin user created/updated');
        } catch (error) {
            console.error('❌ Error creating admin user:', error.message);
        }

        // Create functions for geometry validation and simplification
        await this.createPostGISFunctions();
    }

    async createPostGISFunctions() {
        console.log('🔧 Creating PostGIS utility functions...');
        
        const functions = [
            // Function to update geometry validity
            `CREATE OR REPLACE FUNCTION update_geometry_validity()
             RETURNS VOID AS $$
             BEGIN
                UPDATE voting_districts 
                SET geometry_valid = (geom IS NOT NULL AND ST_IsValid(geom));
             END;
             $$ LANGUAGE plpgsql;`,

            // Function to create simplified geometries for performance
            `CREATE OR REPLACE FUNCTION create_simplified_geometries()
             RETURNS VOID AS $$
             BEGIN
                UPDATE gas_pipelines 
                SET simplified_geom = ST_Simplify(geom, 0.001)
                WHERE geom IS NOT NULL;
                
                UPDATE groundwater_protection 
                SET simplified_geom = ST_Simplify(geom, 0.001)
                WHERE geom IS NOT NULL;
                
                UPDATE conservation_areas 
                SET simplified_geom = ST_Simplify(geom, 0.001)
                WHERE geom IS NOT NULL;
                
                UPDATE settlement_areas 
                SET simplified_geom = ST_Simplify(geom, 0.001)
                WHERE geom IS NOT NULL;
                
                UPDATE highways 
                SET simplified_geom = ST_Simplify(geom, 0.001)
                WHERE geom IS NOT NULL;
                
                UPDATE railways 
                SET simplified_geom = ST_Simplify(geom, 0.001)
                WHERE geom IS NOT NULL;
             END;
             $$ LANGUAGE plpgsql;`,

            // Trigger function for updated_at timestamps
            `CREATE OR REPLACE FUNCTION update_updated_at_column()
             RETURNS TRIGGER AS $$
             BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
             END;
             $$ LANGUAGE plpgsql;`
        ];

        for (const func of functions) {
            try {
                await this.dbClient.query(func);
            } catch (error) {
                console.warn(`Warning creating function: ${error.message}`);
            }
        }

        // Create triggers for updated_at columns
        const tables = [
            'co2_sources', 'voting_districts', 'landfills', 'gravel_pits',
            'wastewater_plants', 'gas_pipelines', 'gas_storage_sites',
            'gas_distribution_points', 'compressor_stations', 'study_area_boundaries',
            'groundwater_protection', 'conservation_areas', 'settlement_areas',
            'highways', 'railways', 'layer_styles', 'admin_users'
        ];

        for (const table of tables) {
            try {
                await this.dbClient.query(`
                    CREATE TRIGGER update_${table}_updated_at 
                    BEFORE UPDATE ON ${table}
                    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
                `);
            } catch (error) {
                // Trigger might already exist, ignore
            }
        }

        console.log('✅ PostGIS utility functions created');
    }
}

// Run setup if called directly
if (require.main === module) {
    const setup = new DatabaseSetup();
    setup.setup();
}

module.exports = DatabaseSetup;