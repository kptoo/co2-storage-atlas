const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const path = require('path');
const { Client } = require('pg');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Database connection
const dbConfig = {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME
};

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

// Start server
app.listen(PORT, () => {
    console.log(`🚀 CO2 Storage Atlas server running on port ${PORT}`);
    console.log(`📍 Access the application at: http://localhost:${PORT}`);
});

module.exports = app;