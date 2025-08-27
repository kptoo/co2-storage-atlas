const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const morgan = require('morgan');
const path = require('path');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const rateLimit = require('express-rate-limit');
const { Pool } = require('pg');
const { body, validationResult } = require('express-validator');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Enhanced database connection configuration for Render
const getDatabaseConfig = () => {
    // Check for DATABASE_URL first (Render's standard environment variable)
    if (process.env.DATABASE_URL) {
        return {
            connectionString: process.env.DATABASE_URL,
            ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
            max: 10, // Reduced pool size for Render
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis: 10000,
            allowExitOnIdle: true,
        };
    }
    
    // Fallback to individual environment variables
    return {
        host: process.env.DB_HOST || 'localhost',
        port: process.env.DB_PORT || 5432,
        user: process.env.DB_USER || 'postgres',
        password: process.env.DB_PASSWORD,
        database: process.env.DB_NAME || 'co2_storage_atlas',
        max: 10,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 10000,
        allowExitOnIdle: true,
        ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
    };
};

// Database connection pool with enhanced configuration
const pool = new Pool(getDatabaseConfig());

// Enhanced database connection handling
pool.on('connect', (client) => {
    console.log('✓ Connected to PostgreSQL database');
});

pool.on('error', (err) => {
    console.error('❌ Database connection error:', err);
    // Don't exit process in production, let health check handle it
    if (process.env.NODE_ENV !== 'production') {
        process.exit(1);
    }
});

// Enhanced database connection test with retry logic
const testDatabaseConnection = async (retries = 3) => {
    for (let i = 0; i < retries; i++) {
        try {
            const client = await pool.connect();
            await client.query('SELECT NOW() as current_time');
            client.release();
            console.log(`✓ Database connection test successful (attempt ${i + 1}/${retries})`);
            return true;
        } catch (error) {
            console.error(`❌ Database connection test failed (attempt ${i + 1}/${retries}):`, error.message);
            if (i < retries - 1) {
                console.log(`⏳ Retrying in ${(i + 1) * 2} seconds...`);
                await new Promise(resolve => setTimeout(resolve, (i + 1) * 2000));
            }
        }
    }
    
    console.error('❌ All database connection attempts failed.');
    return false;
};

// Rate limiting with more appropriate limits for production
const generalLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: process.env.NODE_ENV === 'production' ? 500 : 1000,
    message: 'Too many requests from this IP, please try again later.',
    standardHeaders: true,
    legacyHeaders: false,
});

const adminLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 50,
    message: 'Too many admin requests from this IP, please try again later.',
    standardHeaders: true,
    legacyHeaders: false,
});

// Middleware with production-ready configuration
app.use(helmet({ 
    contentSecurityPolicy: {
        directives: {
            defaultSrc: ["'self'"],
            styleSrc: ["'self'", "'unsafe-inline'", "https://unpkg.com", "https://cdnjs.cloudflare.com"],
            scriptSrc: ["'self'", "'unsafe-inline'", "https://unpkg.com", "https://cdnjs.cloudflare.com"],
            imgSrc: ["'self'", "data:", "https:", "blob:"],
            connectSrc: ["'self'"],
            fontSrc: ["'self'", "https:"],
            objectSrc: ["'none'"],
            mediaSrc: ["'self'"],
            frameSrc: ["'none'"],
        }
    },
    crossOriginEmbedderPolicy: false
}));

app.use(compression());

// Enhanced CORS configuration for production
app.use(cors({
    origin: process.env.NODE_ENV === 'production' 
        ? [
            process.env.FRONTEND_URL,
            /\.onrender\.com$/,
            /localhost:\d+$/,
          ] 
        : true,
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'x-requested-with']
}));

app.use(morgan(process.env.NODE_ENV === 'production' ? 'combined' : 'dev'));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use(generalLimiter);

// Serve static files with proper caching headers
app.use(express.static(path.join(__dirname, 'public'), {
    maxAge: process.env.NODE_ENV === 'production' ? '1d' : '0',
    etag: true,
    lastModified: true
}));

// Trust proxy for Render
if (process.env.NODE_ENV === 'production') {
    app.set('trust proxy', 1);
}

// JWT Authentication middleware
const authenticateToken = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];

    if (!token) {
        return res.status(401).json({ error: 'Access token required' });
    }

    jwt.verify(token, process.env.JWT_SECRET || 'fallback-secret', (err, user) => {
        if (err) {
            return res.status(403).json({ error: 'Invalid token' });
        }
        req.user = user;
        next();
    });
};

// Check if PostGIS is available
const checkPostGIS = async () => {
    try {
        const client = await pool.connect();
        await client.query('SELECT PostGIS_Version()');
        client.release();
        return true;
    } catch (error) {
        console.warn('PostGIS not available, using fallback queries');
        return false;
    }
};

let hasPostGIS = false;

// Enhanced health check endpoint
app.get('/api/health', async (req, res) => {
    try {
        const result = await pool.query('SELECT NOW() as timestamp');
        const dbCheck = await pool.query('SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_schema = $1', ['public']);
        
        // Check PostGIS availability
        let postgisInfo = null;
        try {
            const postgisCheck = await pool.query('SELECT PostGIS_Version() as postgis_version');
            postgisInfo = {
                version: postgisCheck.rows[0].postgis_version,
                available: true
            };
        } catch (postgisError) {
            postgisInfo = { available: false, message: 'PostGIS not installed' };
        }
        
        res.json({ 
            status: 'OK', 
            timestamp: result.rows[0].timestamp,
            database: 'Connected',
            tables: parseInt(dbCheck.rows[0].table_count),
            postgis: postgisInfo,
            environment: process.env.NODE_ENV || 'development',
            port: PORT,
            uptime: process.uptime(),
            database_url_configured: !!process.env.DATABASE_URL
        });
    } catch (error) {
        console.error('Health check failed:', error);
        res.status(503).json({ 
            status: 'ERROR', 
            error: error.message,
            timestamp: new Date().toISOString(),
            environment: process.env.NODE_ENV || 'development',
            database_url_configured: !!process.env.DATABASE_URL
        });
    }
});

// Basic info endpoint for when database is not available
app.get('/api/info', (req, res) => {
    res.json({
        name: 'CO₂ Storage Atlas',
        version: '2.1.0',
        environment: process.env.NODE_ENV || 'development',
        port: PORT,
        uptime: process.uptime(),
        database_url_configured: !!process.env.DATABASE_URL,
        timestamp: new Date().toISOString()
    });
});

// ========================================
// AUTHENTICATION ENDPOINTS
// ========================================

app.post('/api/auth/login', [
    body('username').trim().notEmpty(),
    body('password').notEmpty()
], async (req, res) => {
    try {
        const errors = validationResult(req);
        if (!errors.isEmpty()) {
            return res.status(400).json({ errors: errors.array() });
        }

        const { username, password } = req.body;
        
        // Check if admin_users table exists
        const tableCheck = await pool.query(`
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = 'admin_users'
            )
        `);
        
        if (!tableCheck.rows[0].exists) {
            return res.status(503).json({ error: 'Admin functionality not configured' });
        }
        
        const result = await pool.query(
            'SELECT id, username, password_hash FROM admin_users WHERE username = $1 AND is_active = true',
            [username]
        );

        if (result.rows.length === 0) {
            return res.status(401).json({ error: 'Invalid credentials' });
        }

        const user = result.rows[0];
        const isValidPassword = await bcrypt.compare(password, user.password_hash);

        if (!isValidPassword) {
            return res.status(401).json({ error: 'Invalid credentials' });
        }

        // Update last login
        await pool.query(
            'UPDATE admin_users SET last_login = CURRENT_TIMESTAMP WHERE id = $1',
            [user.id]
        );

        const token = jwt.sign(
            { id: user.id, username: user.username },
            process.env.JWT_SECRET || 'fallback-secret',
            { expiresIn: process.env.JWT_EXPIRES_IN || '24h' }
        );

        res.json({ 
            token, 
            user: { id: user.id, username: user.username }
        });
    } catch (error) {
        console.error('Login error:', error);
        res.status(500).json({ error: 'Login failed' });
    }
});

// ========================================
// DATA RETRIEVAL ENDPOINTS WITH FALLBACKS
// ========================================

// Helper function to check if table exists
const tableExists = async (tableName) => {
    try {
        const result = await pool.query(`
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = $1
            )
        `, [tableName]);
        return result.rows[0].exists;
    } catch (error) {
        console.error(`Error checking table ${tableName}:`, error);
        return false;
    }
};

// Helper function to check if column exists
const columnExists = async (tableName, columnName) => {
    try {
        const result = await pool.query(`
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2
            )
        `, [tableName, columnName]);
        return result.rows[0].exists;
    } catch (error) {
        console.error(`Error checking column ${columnName} in ${tableName}:`, error);
        return false;
    }
};

// CO2 Sources with enhanced performance and fallbacks
app.get('/api/co2-sources-enhanced', async (req, res) => {
    try {
        // Check if table exists
        if (!(await tableExists('co2_sources'))) {
            return res.json([]); // Return empty array if table doesn't exist
        }

        const { bbox, zoom } = req.query;
        
        // Build query with fallbacks for missing columns
        const hasGeom = await columnExists('co2_sources', 'geom');
        const hasLongitude = await columnExists('co2_sources', 'longitude');
        const hasLatitude = await columnExists('co2_sources', 'latitude');
        const hasPinSize = await columnExists('co2_sources', 'pin_size');
        const hasPinColor = await columnExists('co2_sources', 'pin_color');
        const hasProminent = await columnExists('co2_sources', 'is_prominent');
        
        let query = `
            SELECT id, 
                   plant_name, 
                   plant_type, 
                   COALESCE(total_co2_t, 0) as total_co2_t, 
                   COALESCE(fossil_co2_t, 0) as fossil_co2_t,
                   COALESCE(biogenic_co2_t, 0) as biogenic_co2_t, 
                   COALESCE(comment, '') as comment,
                   ${hasProminent ? 'COALESCE(is_prominent, false) as is_prominent,' : 'false as is_prominent,'}
                   ${hasPinSize ? 'COALESCE(pin_size, 2) as pin_size,' : '2 as pin_size,'}
                   ${hasPinColor ? 'COALESCE(pin_color, \'#ff0000\') as pin_color,' : '\'#ff0000\' as pin_color,'}
        `;
        
        if (hasPostGIS && hasGeom) {
            query += `
                   ST_X(geom) as longitude, 
                   ST_Y(geom) as latitude
            FROM co2_sources
            WHERE geom IS NOT NULL
            `;
        } else if (hasLongitude && hasLatitude) {
            query += `
                   longitude, 
                   latitude
            FROM co2_sources
            WHERE longitude IS NOT NULL AND latitude IS NOT NULL
            `;
        } else {
            // No geographic data available
            query += `
                   0 as longitude, 
                   0 as latitude
            FROM co2_sources
            `;
        }
        
        const params = [];
        
        // Add bounding box filter if PostGIS is available
        if (bbox && hasPostGIS && hasGeom) {
            const [minLng, minLat, maxLng, maxLat] = bbox.split(',').map(Number);
            query += ` AND geom && ST_MakeEnvelope($${params.length + 1}, $${params.length + 2}, $${params.length + 3}, $${params.length + 4}, 4326)`;
            params.push(minLng, minLat, maxLng, maxLat);
        }
        
        query += ` ORDER BY total_co2_t DESC NULLS LAST`;
        
        // Limit results at low zoom levels for performance
        if (zoom && parseInt(zoom) < 10) {
            query += ` LIMIT 1000`;
        }
        
        const result = await pool.query(query, params);
        
        console.log(`Retrieved ${result.rows.length} CO2 sources`);
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching CO2 sources:', error);
        res.status(500).json({ 
            error: 'Failed to fetch CO2 sources',
            details: process.env.NODE_ENV === 'development' ? error.message : 'Internal server error'
        });
    }
});

// Voting districts endpoint with fallbacks
app.get('/api/voting-districts-choropleth', async (req, res) => {
    try {
        if (!(await tableExists('voting_districts'))) {
            return res.json([]);
        }

        const { simplify } = req.query;
        const hasGeom = await columnExists('voting_districts', 'geom');
        
        if (!hasPostGIS || !hasGeom) {
            return res.json([]);
        }
        
        const tolerance = simplify === 'true' ? 0.001 : 0;
        
        const query = `
            SELECT vd.id, 
                   COALESCE(vd.gkz, '') as gkz, 
                   COALESCE(vd.name, '') as name, 
                   COALESCE(vd.spo_percent, 0) as spo_percent, 
                   COALESCE(vd.ovp_percent, 0) as ovp_percent, 
                   COALESCE(vd.fpo_percent, 0) as fpo_percent,
                   COALESCE(vd.grune_percent, 0) as grune_percent, 
                   COALESCE(vd.kpo_percent, 0) as kpo_percent, 
                   COALESCE(vd.neos_percent, 0) as neos_percent,
                   COALESCE(vd.left_green_combined, 0) as left_green_combined, 
                   COALESCE(vd.choropleth_color, '#cccccc') as fill_color,
                   ${tolerance > 0 
                     ? `ST_AsGeoJSON(ST_Simplify(ST_Transform(vd.geom, 4326), ${tolerance}))` 
                     : 'ST_AsGeoJSON(ST_Transform(vd.geom, 4326))'
                   } as geometry,
                   ST_X(ST_Transform(ST_Centroid(vd.geom), 4326)) as center_lng,
                   ST_Y(ST_Transform(ST_Centroid(vd.geom), 4326)) as center_lat
            FROM voting_districts vd
            WHERE vd.geom IS NOT NULL
            ORDER BY vd.left_green_combined DESC NULLS LAST
        `;
        
        const result = await pool.query(query);
        
        console.log(`Retrieved ${result.rows.length} voting districts`);
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching voting districts:', error);
        res.status(500).json({ 
            error: 'Failed to fetch voting districts',
            details: process.env.NODE_ENV === 'development' ? error.message : 'Internal server error'
        });
    }
});

// Generic endpoint for point-based layers with enhanced fallbacks
const createPointLayerEndpoint = (tableName, fields, orderBy = 'id') => {
    return async (req, res) => {
        try {
            if (!(await tableExists(tableName))) {
                return res.json([]);
            }

            const { bbox, zoom } = req.query;
            const hasGeom = await columnExists(tableName, 'geom');
            const hasLongitude = await columnExists(tableName, 'longitude');
            const hasLatitude = await columnExists(tableName, 'latitude');
            
            // Build base fields, checking if each exists
            const safeFields = [];
            for (const field of fields) {
                // Extract column name from field (handle COALESCE statements)
                const columnName = field.includes('(') ? 
                    field.split('(')[1].split(',')[0].trim() : 
                    field.split(' as ')[0].trim();
                
                if (field.includes('COALESCE') || await columnExists(tableName, columnName)) {
                    safeFields.push(field);
                }
            }
            
            let query = `SELECT ${safeFields.join(', ')},`;
            
            if (hasPostGIS && hasGeom) {
                query += `
                       ST_X(ST_Transform(geom, 4326)) as longitude, 
                       ST_Y(ST_Transform(geom, 4326)) as latitude
                FROM ${tableName}
                WHERE geom IS NOT NULL
                `;
            } else if (hasLongitude && hasLatitude) {
                query += `
                       longitude, 
                       latitude
                FROM ${tableName}
                WHERE longitude IS NOT NULL AND latitude IS NOT NULL
                `;
            } else {
                query += `
                       0 as longitude, 
                       0 as latitude
                FROM ${tableName}
                `;
            }
            
            const params = [];
            
            // Add bounding box filter if PostGIS is available
            if (bbox && hasPostGIS && hasGeom) {
                const [minLng, minLat, maxLng, maxLat] = bbox.split(',').map(Number);
                query += ` AND geom && ST_Transform(ST_MakeEnvelope($${params.length + 1}, $${params.length + 2}, $${params.length + 3}, $${params.length + 4}, 4326), ST_SRID(geom))`;
                params.push(minLng, minLat, maxLng, maxLat);
            }
            
            // Check if orderBy column exists
            if (await columnExists(tableName, orderBy)) {
                query += ` ORDER BY ${orderBy}`;
            }
            
            // Add limit for performance at low zoom levels
            if (zoom && parseInt(zoom) < 8) {
                query += ` LIMIT 500`;
            }
            
            const result = await pool.query(query, params);
            
            console.log(`Retrieved ${result.rows.length} ${tableName.replace('_', ' ')}`);
            res.json(result.rows);
        } catch (error) {
            console.error(`Error fetching ${tableName}:`, error);
            res.status(500).json({ 
                error: `Failed to fetch ${tableName.replace('_', ' ')}`,
                details: process.env.NODE_ENV === 'development' ? error.message : 'Internal server error'
            });
        }
    };
};

// Point-based layer endpoints with fallback-safe fields
app.get('/api/landfills-enhanced', createPointLayerEndpoint('landfills', [
    'id', 
    'COALESCE(company_name, \'\') as company_name', 
    'COALESCE(location_name, \'\') as location_name', 
    'COALESCE(district, \'\') as district', 
    'COALESCE(address, \'\') as address', 
    'COALESCE(facility_type, \'\') as facility_type',
    '2 as pin_size', 
    '\'#ff8800\' as pin_color',
    '0.8 as opacity'
]));

app.get('/api/gravel-pits-enhanced', createPointLayerEndpoint('gravel_pits', [
    'id', 
    'COALESCE(name, \'\') as name', 
    'COALESCE(resource, \'\') as resource', 
    'COALESCE(tags, \'\') as tags',
    '2 as pin_size', 
    '\'#8855aa\' as pin_color',
    '0.7 as opacity'
]));

app.get('/api/wastewater-plants-enhanced', createPointLayerEndpoint('wastewater_plants', [
    'id', 
    'COALESCE(pk, 0) as pk', 
    'COALESCE(label, \'\') as label', 
    'COALESCE(treatment_type, \'\') as treatment_type', 
    'COALESCE(capacity, 0) as capacity',
    '2 as pin_size', 
    '\'#3388ff\' as pin_color',
    '0.6 as opacity'
]));

app.get('/api/gas-storage-sites-enhanced', createPointLayerEndpoint('gas_storage_sites', [
    'id', 
    'COALESCE(name, \'\') as name', 
    'COALESCE(operator, \'\') as operator', 
    'COALESCE(storage_type, \'\') as storage_type', 
    'COALESCE(capacity_bcm, 0) as capacity_bcm',
    '2 as pin_size', 
    '\'#00cc88\' as pin_color',
    '0.5 as opacity'
]));

app.get('/api/gas-distribution-points-enhanced', createPointLayerEndpoint('gas_distribution_points', [
    'id', 
    'COALESCE(name, \'\') as name', 
    'COALESCE(type, \'\') as type', 
    'COALESCE(operator, \'\') as operator',
    '1 as pin_size', 
    '\'#00aa44\' as pin_color',
    '0.4 as opacity'
]));

app.get('/api/compressor-stations-enhanced', createPointLayerEndpoint('compressor_stations', [
    'id', 
    'COALESCE(name, \'\') as name', 
    'COALESCE(operator, \'\') as operator', 
    'COALESCE(capacity_info, \'\') as capacity_info',
    '2 as pin_size', 
    '\'#ffaa00\' as pin_color',
    '0.3 as opacity'
]));

// Generic endpoint for line-based layers with fallbacks
const createLineLayerEndpoint = (tableName, fields) => {
    return async (req, res) => {
        try {
            if (!(await tableExists(tableName))) {
                return res.json([]);
            }

            const hasGeom = await columnExists(tableName, 'geom');
            
            if (!hasPostGIS || !hasGeom) {
                return res.json([]);
            }

            const { bbox, simplify } = req.query;
            const tolerance = simplify === 'true' ? 0.001 : 0;
            
            // Build safe fields
            const safeFields = [];
            for (const field of fields) {
                const columnName = field.includes('(') ? 
                    field.split('(')[1].split(',')[0].trim() : 
                    field.split(' as ')[0].trim();
                
                if (field.includes('COALESCE') || await columnExists(tableName, columnName)) {
                    safeFields.push(field);
                }
            }
            
            let geomField = 'geom';
            if (tolerance > 0) {
                geomField = `ST_Simplify(geom, ${tolerance})`;
            }
            
            let query = `
                SELECT ${safeFields.join(', ')},
                       ST_AsGeoJSON(ST_Transform(${geomField}, 4326)) as geometry
                FROM ${tableName}
                WHERE geom IS NOT NULL
            `;
            
            const params = [];
            
            if (bbox) {
                const [minLng, minLat, maxLng, maxLat] = bbox.split(',').map(Number);
                query += ` AND geom && ST_Transform(ST_MakeEnvelope(${params.length + 1}, ${params.length + 2}, ${params.length + 3}, ${params.length + 4}, 4326), ST_SRID(geom))`;
                params.push(minLng, minLat, maxLng, maxLat);
            }
            
            const result = await pool.query(query, params);
            
            console.log(`Retrieved ${result.rows.length} ${tableName.replace('_', ' ')}`);
            res.json(result.rows);
        } catch (error) {
            console.error(`Error fetching ${tableName}:`, error);
            res.status(500).json({ 
                error: `Failed to fetch ${tableName.replace('_', ' ')}`,
                details: process.env.NODE_ENV === 'development' ? error.message : 'Internal server error'
            });
        }
    };
};

// Polygon-based layer endpoints
app.get('/api/groundwater-protection', createPolygonLayerEndpoint('groundwater_protection', [
    'id', 
    'COALESCE(name, \'\') as name', 
    'COALESCE(protection_zone, \'\') as protection_zone',
    '\'#0066ff\' as fill_color', 
    '0.3 as fill_opacity', 
    '\'#0044cc\' as border_color', 
    '2 as border_weight'
]));

app.get('/api/conservation-areas', createPolygonLayerEndpoint('conservation_areas', [
    'id', 
    'COALESCE(name, \'\') as name', 
    'COALESCE(protection_level, \'\') as protection_level', 
    'COALESCE(area_type, \'\') as area_type',
    '\'#00ff00\' as fill_color', 
    '0.3 as fill_opacity', 
    '\'#00cc00\' as border_color', 
    '2 as border_weight'
]));

app.get('/api/settlement-areas', createPolygonLayerEndpoint('settlement_areas', [
    'id', 
    'COALESCE(name, \'\') as name', 
    'COALESCE(area_type, \'\') as area_type', 
    'COALESCE(population, 0) as population',
    '\'#ff0000\' as fill_color', 
    '0.3 as fill_opacity', 
    '\'#cc0000\' as border_color', 
    '2 as border_weight'
]));

// Database stats endpoint with table existence checks
app.get('/api/database-stats', async (req, res) => {
    try {
        const tables = [
            'co2_sources', 'voting_districts', 'landfills', 'gravel_pits',
            'wastewater_plants', 'gas_pipelines', 'gas_storage_sites',
            'gas_distribution_points', 'compressor_stations',
            'groundwater_protection', 'conservation_areas', 'settlement_areas',
            'highways', 'railways'
        ];
        
        const stats = {};
        for (const table of tables) {
            try {
                if (!(await tableExists(table))) {
                    stats[table] = { total: 0, validGeometry: 0, exists: false };
                    continue;
                }
                
                const countResult = await pool.query(`SELECT COUNT(*) as count FROM ${table}`);
                
                let validGeomCount = 0;
                const hasGeom = await columnExists(table, 'geom');
                if (hasPostGIS && hasGeom) {
                    const validGeomResult = await pool.query(`SELECT COUNT(*) as count FROM ${table} WHERE geom IS NOT NULL`);
                    validGeomCount = parseInt(validGeomResult.rows[0].count);
                }
                
                stats[table] = {
                    total: parseInt(countResult.rows[0].count),
                    validGeometry: validGeomCount,
                    exists: true,
                    hasGeometry: hasGeom
                };
            } catch (error) {
                stats[table] = { total: 0, validGeometry: 0, exists: false, error: error.message };
            }
        }
        
        res.json(stats);
    } catch (error) {
        console.error('Error fetching database stats:', error);
        res.status(500).json({ 
            error: 'Failed to fetch database statistics',
            details: process.env.NODE_ENV === 'development' ? error.message : 'Internal server error'
        });
    }
});

// Layer styles endpoint with table existence check
app.get('/api/layer-styles', async (req, res) => {
    try {
        if (!(await tableExists('layer_styles'))) {
            return res.json([]);
        }
        
        const query = 'SELECT * FROM layer_styles WHERE is_active = true';
        const result = await pool.query(query);
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching layer styles:', error);
        res.status(500).json({ 
            error: 'Failed to fetch layer styles',
            details: process.env.NODE_ENV === 'development' ? error.message : 'Internal server error'
        });
    }
});

// ========================================
// ADMIN ENDPOINTS
// ========================================

// Add CO2 source
app.post('/api/admin/co2-sources', [
    adminLimiter,
    body('plant_name').trim().notEmpty(),
    body('plant_type').trim().notEmpty(),
    body('latitude').isFloat({ min: 46, max: 49 }),
    body('longitude').isFloat({ min: 9, max: 17 }),
    body('total_co2_t').isFloat({ min: 0 }),
    body('fossil_co2_t').isFloat({ min: 0 }),
    body('biogenic_co2_t').isFloat({ min: 0 })
], authenticateToken, async (req, res) => {
    try {
        const errors = validationResult(req);
        if (!errors.isEmpty()) {
            return res.status(400).json({ errors: errors.array() });
        }

        if (!(await tableExists('co2_sources'))) {
            return res.status(503).json({ error: 'CO2 sources table not available' });
        }

        const {
            plant_name, plant_type, total_co2_t, fossil_co2_t,
            biogenic_co2_t, latitude, longitude, comment
        } = req.body;

        const isProminent = total_co2_t > 50000;
        const pinSize = isProminent ? 4 : 2;

        let query, values;
        
        if (hasPostGIS && await columnExists('co2_sources', 'geom')) {
            query = `
                INSERT INTO co2_sources (
                    plant_name, plant_type, total_co2_t, fossil_co2_t,
                    biogenic_co2_t, comment, is_prominent, pin_size, geom
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 
                    ST_SetSRID(ST_MakePoint($9, $10), 4326))
                RETURNING *
            `;
            values = [
                plant_name, plant_type, total_co2_t, fossil_co2_t,
                biogenic_co2_t, comment || '', isProminent, pinSize,
                longitude, latitude
            ];
        } else {
            // Fallback without PostGIS
            query = `
                INSERT INTO co2_sources (
                    plant_name, plant_type, total_co2_t, fossil_co2_t,
                    biogenic_co2_t, comment, is_prominent, pin_size,
                    longitude, latitude
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                RETURNING *
            `;
            values = [
                plant_name, plant_type, total_co2_t, fossil_co2_t,
                biogenic_co2_t, comment || '', isProminent, pinSize,
                longitude, latitude
            ];
        }

        const result = await pool.query(query, values);

        // Log to audit table if it exists
        try {
            if (await tableExists('audit_log')) {
                await pool.query(`
                    INSERT INTO audit_log (table_name, record_id, action, new_values, user_id)
                    VALUES ($1, $2, $3, $4, $5)
                `, ['co2_sources', result.rows[0].id, 'INSERT', JSON.stringify(req.body), req.user.id]);
            }
        } catch (auditError) {
            console.log('Audit logging not available:', auditError.message);
        }

        res.status(201).json(result.rows[0]);
    } catch (error) {
        console.error('Error creating CO2 source:', error);
        res.status(500).json({ error: 'Failed to create CO2 source' });
    }
});

// Update CO2 source
app.put('/api/admin/co2-sources/:id', [
    adminLimiter,
    body('plant_name').optional().trim().notEmpty(),
    body('plant_type').optional().trim().notEmpty(),
    body('latitude').optional().isFloat({ min: 46, max: 49 }),
    body('longitude').optional().isFloat({ min: 9, max: 17 })
], authenticateToken, async (req, res) => {
    try {
        const errors = validationResult(req);
        if (!errors.isEmpty()) {
            return res.status(400).json({ errors: errors.array() });
        }

        const { id } = req.params;
        const updates = req.body;

        if (!(await tableExists('co2_sources'))) {
            return res.status(503).json({ error: 'CO2 sources table not available' });
        }

        // Get old values for audit
        const oldRecord = await pool.query('SELECT * FROM co2_sources WHERE id = $1', [id]);
        if (oldRecord.rows.length === 0) {
            return res.status(404).json({ error: 'CO2 source not found' });
        }

        // Build dynamic update query
        const setClauses = [];
        const values = [];
        let paramIndex = 1;

        Object.keys(updates).forEach(key => {
            if (key === 'latitude' || key === 'longitude') {
                return;
            }
            setClauses.push(`${key} = ${paramIndex}`);
            values.push(updates[key]);
            paramIndex++;
        });

        if (updates.latitude && updates.longitude && hasPostGIS && await columnExists('co2_sources', 'geom')) {
            setClauses.push(`geom = ST_SetSRID(ST_MakePoint(${paramIndex}, ${paramIndex + 1}), 4326)`);
            values.push(updates.longitude, updates.latitude);
            paramIndex += 2;
        } else if (updates.latitude && updates.longitude) {
            setClauses.push(`longitude = ${paramIndex}, latitude = ${paramIndex + 1}`);
            values.push(updates.longitude, updates.latitude);
            paramIndex += 2;
        }

        if (setClauses.length === 0) {
            return res.status(400).json({ error: 'No valid fields to update' });
        }

        values.push(id);
        const query = `
            UPDATE co2_sources 
            SET ${setClauses.join(', ')}, updated_at = CURRENT_TIMESTAMP
            WHERE id = ${paramIndex}
            RETURNING *
        `;

        const result = await pool.query(query, values);

        // Log to audit table if it exists
        try {
            if (await tableExists('audit_log')) {
                await pool.query(`
                    INSERT INTO audit_log (table_name, record_id, action, old_values, new_values, user_id)
                    VALUES ($1, $2, $3, $4, $5, $6)
                `, ['co2_sources', id, 'UPDATE', JSON.stringify(oldRecord.rows[0]), JSON.stringify(updates), req.user.id]);
            }
        } catch (auditError) {
            console.log('Audit logging not available:', auditError.message);
        }

        res.json(result.rows[0]);
    } catch (error) {
        console.error('Error updating CO2 source:', error);
        res.status(500).json({ error: 'Failed to update CO2 source' });
    }
});

// Delete CO2 source
app.delete('/api/admin/co2-sources/:id', adminLimiter, authenticateToken, async (req, res) => {
    try {
        const { id } = req.params;

        if (!(await tableExists('co2_sources'))) {
            return res.status(503).json({ error: 'CO2 sources table not available' });
        }

        // Get record for audit
        const oldRecord = await pool.query('SELECT * FROM co2_sources WHERE id = $1', [id]);
        if (oldRecord.rows.length === 0) {
            return res.status(404).json({ error: 'CO2 source not found' });
        }

        await pool.query('DELETE FROM co2_sources WHERE id = $1', [id]);

        // Log to audit table if it exists
        try {
            if (await tableExists('audit_log')) {
                await pool.query(`
                    INSERT INTO audit_log (table_name, record_id, action, old_values, user_id)
                    VALUES ($1, $2, $3, $4, $5)
                `, ['co2_sources', id, 'DELETE', JSON.stringify(oldRecord.rows[0]), req.user.id]);
            }
        } catch (auditError) {
            console.log('Audit logging not available:', auditError.message);
        }

        res.json({ message: 'CO2 source deleted successfully' });
    } catch (error) {
        console.error('Error deleting CO2 source:', error);
        res.status(500).json({ error: 'Failed to delete CO2 source' });
    }
});

// Get existing CO2 sources for editing
app.get('/api/admin/co2-sources', authenticateToken, async (req, res) => {
    try {
        if (!(await tableExists('co2_sources'))) {
            return res.json([]);
        }

        const hasGeom = await columnExists('co2_sources', 'geom');
        const hasLongitude = await columnExists('co2_sources', 'longitude');
        const hasLatitude = await columnExists('co2_sources', 'latitude');
        
        let query = `
            SELECT id, plant_name, plant_type, total_co2_t, fossil_co2_t, biogenic_co2_t, comment,
        `;
        
        if (hasPostGIS && hasGeom) {
            query += ` ST_X(geom) as longitude, ST_Y(geom) as latitude`;
        } else if (hasLongitude && hasLatitude) {
            query += ` longitude, latitude`;
        } else {
            query += ` 0 as longitude, 0 as latitude`;
        }
        
        query += ` FROM co2_sources ORDER BY plant_name`;
        
        const result = await pool.query(query);
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching CO2 sources for admin:', error);
        res.status(500).json({ error: 'Failed to fetch CO2 sources' });
    }
});

// Database optimization endpoint
app.post('/api/admin/optimize-database', adminLimiter, authenticateToken, async (req, res) => {
    try {
        const optimizations = [];
        
        // Analyze tables
        const tables = [
            'co2_sources', 'voting_districts', 'landfills', 'gravel_pits',
            'wastewater_plants', 'gas_pipelines', 'gas_storage_sites',
            'gas_distribution_points', 'compressor_stations',
            'groundwater_protection', 'conservation_areas', 'settlement_areas',
            'highways', 'railways'
        ];

        for (const table of tables) {
            try {
                if (await tableExists(table)) {
                    await pool.query(`ANALYZE ${table}`);
                    optimizations.push(`Analyzed ${table}`);
                } else {
                    optimizations.push(`Skipped ${table} (table does not exist)`);
                }
            } catch (error) {
                optimizations.push(`Failed to analyze ${table}: ${error.message}`);
            }
        }

        res.json({ 
            message: 'Database optimization completed',
            optimizations
        });
    } catch (error) {
        console.error('Error optimizing database:', error);
        res.status(500).json({ error: 'Failed to optimize database' });
    }
});

// Get audit log if table exists
app.get('/api/admin/audit-log', adminLimiter, authenticateToken, async (req, res) => {
    try {
        if (!(await tableExists('audit_log'))) {
            return res.json([]);
        }

        const { limit = 100, offset = 0, table_name } = req.query;
        
        let query = `
            SELECT al.*, au.username
            FROM audit_log al
            LEFT JOIN admin_users au ON al.user_id = au.id
        `;
        
        const params = [];
        
        if (table_name) {
            query += ` WHERE al.table_name = ${params.length + 1}`;
            params.push(table_name);
        }
        
        query += ` ORDER BY al.timestamp DESC LIMIT ${params.length + 1} OFFSET ${params.length + 2}`;
        params.push(limit, offset);

        const result = await pool.query(query, params);
        
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching audit log:', error);
        res.status(500).json({ error: 'Failed to fetch audit log' });
    }
});

// Root route serves the main application
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Catch-all handler for SPA routing
app.get('*', (req, res) => {
    // Don't serve index.html for API routes
    if (req.path.startsWith('/api/')) {
        return res.status(404).json({ error: 'API endpoint not found' });
    }
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Global error handler
app.use((error, req, res, next) => {
    console.error('Unhandled error:', error);
    res.status(500).json({
        error: 'Internal server error',
        message: process.env.NODE_ENV === 'development' ? error.message : 'Something went wrong'
    });
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
    console.error('Uncaught Exception:', error);
    if (process.env.NODE_ENV === 'production') {
        console.error('Application will continue running...');
    } else {
        process.exit(1);
    }
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    if (process.env.NODE_ENV === 'production') {
        console.error('Application will continue running...');
    }
});

// Graceful shutdown
const gracefulShutdown = (signal) => {
    console.log(`Received ${signal}, shutting down gracefully...`);
    pool.end(() => {
        console.log('Database pool closed');
        process.exit(0);
    });
    
    // Force exit after 30 seconds
    setTimeout(() => {
        console.error('Could not close connections in time, forcefully shutting down');
        process.exit(1);
    }, 30000);
};

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Start server with enhanced logging and PostGIS detection
const startServer = async () => {
    try {
        await testDatabaseConnection();
        hasPostGIS = await checkPostGIS();
        
        app.listen(PORT, '0.0.0.0', () => {
            console.log(`
========================================
CO₂ Storage Atlas Server Started
========================================
Environment: ${process.env.NODE_ENV || 'development'}
Port: ${PORT}
Database: ${process.env.DB_NAME || 'co2_storage_atlas'}
Host: ${process.env.DB_HOST || 'localhost'}:${process.env.DB_PORT || 5432}
DATABASE_URL: ${process.env.DATABASE_URL ? 'Configured' : 'Not configured'}
PostGIS: ${hasPostGIS ? 'Available' : 'Not available'}
JWT Secret: ${process.env.JWT_SECRET ? 'Configured' : 'Using default'}
SSL: ${process.env.NODE_ENV === 'production' ? 'Enabled' : 'Disabled'}
========================================
Features:
- Enhanced error handling for missing tables/columns
- PostGIS detection with fallbacks
- Table existence validation
- Production-ready for Render deployment
- Graceful degradation when features unavailable
========================================
Health Check: /api/health
Info Endpoint: /api/info
Main App: ${process.env.NODE_ENV === 'production' ? 'https://your-app.onrender.com' : `http://localhost:${PORT}`}
========================================
            `);
        });
    } catch (error) {
        console.error('Failed to start server:', error);
        if (process.env.NODE_ENV !== 'production') {
            process.exit(1);
        }
    }
};

startServer();

module.exports = app;].trim();
                
                if (field.includes('COALESCE') || await columnExists(tableName, columnName)) {
                    safeFields.push(field);
                }
            }
            
            let geomField = 'geom';
            if (tolerance > 0) {
                geomField = `ST_Simplify(geom, ${tolerance})`;
            }
            
            let query = `
                SELECT ${safeFields.join(', ')},
                       ST_AsGeoJSON(ST_Transform(${geomField}, 4326)) as geometry
                FROM ${tableName}
                WHERE geom IS NOT NULL
            `;
            
            const params = [];
            
            if (bbox) {
                const [minLng, minLat, maxLng, maxLat] = bbox.split(',').map(Number);
                query += ` AND geom && ST_Transform(ST_MakeEnvelope($${params.length + 1}, $${params.length + 2}, $${params.length + 3}, $${params.length + 4}, 4326), ST_SRID(geom))`;
                params.push(minLng, minLat, maxLng, maxLat);
            }
            
            const result = await pool.query(query, params);
            
            console.log(`Retrieved ${result.rows.length} ${tableName.replace('_', ' ')}`);
            res.json(result.rows);
        } catch (error) {
            console.error(`Error fetching ${tableName}:`, error);
            res.status(500).json({ 
                error: `Failed to fetch ${tableName.replace('_', ' ')}`,
                details: process.env.NODE_ENV === 'development' ? error.message : 'Internal server error'
            });
        }
    };
};

// Line-based layer endpoints
app.get('/api/gas-pipelines-enhanced', createLineLayerEndpoint('gas_pipelines', [
    'id', 
    'COALESCE(name, \'\') as name', 
    'COALESCE(operator, \'\') as operator', 
    'COALESCE(diameter, \'\') as diameter', 
    'COALESCE(pressure_level, \'\') as pressure_level', 
    'COALESCE(pipeline_type, \'\') as pipeline_type',
    '\'#00aa44\' as line_color', 
    '4 as line_weight', 
    '0.8 as line_opacity'
]));

app.get('/api/highways', createLineLayerEndpoint('highways', [
    'id', 
    'COALESCE(name, \'\') as name', 
    'COALESCE(highway_number, \'\') as highway_number', 
    'COALESCE(road_type, \'\') as road_type',
    '\'#666666\' as line_color', 
    '3 as line_weight', 
    '0.7 as line_opacity'
]));

app.get('/api/railways', createLineLayerEndpoint('railways', [
    'id', 
    'COALESCE(name, \'\') as name', 
    'COALESCE(railway_type, \'\') as railway_type', 
    'COALESCE(operator, \'\') as operator',
    '\'#8B4513\' as line_color', 
    '3 as line_weight', 
    '0.8 as line_opacity'
]));

// Generic endpoint for polygon-based layers with fallbacks
const createPolygonLayerEndpoint = (tableName, fields) => {
    return async (req, res) => {
        try {
            if (!(await tableExists(tableName))) {
                return res.json([]);
            }

            const hasGeom = await columnExists(tableName, 'geom');
            
            if (!hasPostGIS || !hasGeom) {
                return res.json([]);
            }

            const { bbox, simplify } = req.query;
            const tolerance = simplify === 'true' ? 0.002 : 0; // Larger tolerance for polygons
            
            // Build safe fields
            const safeFields = [];
            for (const field of fields) {
                const columnName = field.includes('(') ? 
                    field.split('(')[1].split(',')[0].trim() : 
                    field.split(' as ')[0
