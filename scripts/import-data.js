const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const XLSX = require('xlsx');
const shapefile = require('shapefile');
const proj4 = require('proj4');
const turf = require('@turf/turf');
const { Client } = require('pg');
require('dotenv').config();

class OptimizedDataImporter {
    constructor() {
        this.client = new Client({
            host: process.env.DB_HOST || 'localhost',
            port: process.env.DB_PORT || 5432,
            user: process.env.DB_USER || 'postgres',
            password: process.env.DB_PASSWORD,
            database: process.env.DB_NAME || 'co2_storage_atlas',
        });

        // Data directories
        this.dataDir = path.join(__dirname, '..', 'zurich_data');
        this.shapefileDir = path.join(__dirname, '..', 'Shapefiles');
        this.unsuitableDir = path.join(__dirname, '..', 'Unsuitable');
        this.roadsDir = path.join(__dirname, '..', 'roads');
        this.railwayDir = path.join(__dirname, '..', 'railway');
        this.areaOfInterestDir = path.join(__dirname, '..', 'Area Of Interest');

        // Austrian coordinate systems
        this.projections = {
            'EPSG:31287': '+proj=lcc +lat_1=49 +lat_2=46 +lat_0=47.5 +lon_0=13.33333333333333 +x_0=400000 +y_0=400000 +ellps=bessel +towgs84=577.326,90.129,463.919,5.137,1.474,5.297,2.4232 +units=m +no_defs',
            'EPSG:31259': '+proj=tmerc +lat_0=0 +lon_0=10.33333333333333 +k=1 +x_0=150000 +y_0=-5000000 +ellps=bessel +towgs84=577.326,90.129,463.919,5.137,1.474,5.297,2.4232 +units=m +no_defs',
            'EPSG:3416': '+proj=tmerc +lat_0=0 +lon_0=16 +k=1 +x_0=500000 +y_0=-5000000 +ellps=bessel +towgs84=577.326,90.129,463.919,5.137,1.474,5.297,2.4232 +units=m +no_defs'
        };

        this.stats = {
            co2Sources: 0,
            votingDistricts: 0,
            landfills: 0,
            gravelPits: 0,
            wastewaterPlants: 0,
            gasPipelines: 0,
            gasStorage: 0,
            gasDistribution: 0,
            compressorStations: 0,
            groundwaterAreas: 0,
            conservationAreas: 0,
            residentialAreas: 0,
            roads: 0,
            railways: 0,
            errors: 0,
            filteredOutByArea: 0
        };

        this.areaOfInterestBounds = null;
    }

    async importAllData() {
        try {
            console.log('Starting optimized data import...\n');
            await this.client.connect();

            await this.loadAreaBounds();
            await this.clearExistingData();
            await this.importBoundaries();
            await this.importUpdatedVotingDistricts(); // Updated: use shapefile directly
            await this.importCSVExcelData();
            await this.importShapefileData();
            await this.importUnsuitableAreasOptimized();
            await this.importTransportInfrastructure();
            await this.createMaterializedViews();
            
            this.printSummary();
            console.log('\nOptimized data import completed successfully!');

        } catch (error) {
            console.error('Import failed:', error);
            this.stats.errors++;
        } finally {
            await this.client.end();
        }
    }

    async loadAreaBounds() {
        console.log('Loading simplified area bounds...');
        
        const areaFiles = [
            path.join(this.areaOfInterestDir, 'salzburg.shp'),
            path.join(this.areaOfInterestDir, 'upper_austria.shp')
        ];

        let combinedBounds = null;

        for (const file of areaFiles) {
            if (fs.existsSync(file)) {
                console.log(`  Processing ${path.basename(file)}...`);
                try {
                    await shapefile.read(file).then(collection => {
                        collection.features.forEach(feature => {
                            const geometry = this.transformGeometry(feature.geometry);
                            if (this.isValidGeoJSON(geometry)) {
                                const bounds = turf.bbox(turf.feature(geometry));
                                if (!combinedBounds) {
                                    combinedBounds = bounds;
                                } else {
                                    combinedBounds[0] = Math.min(combinedBounds[0], bounds[0]);
                                    combinedBounds[1] = Math.min(combinedBounds[1], bounds[1]);
                                    combinedBounds[2] = Math.max(combinedBounds[2], bounds[2]);
                                    combinedBounds[3] = Math.max(combinedBounds[3], bounds[3]);
                                }
                            }
                        });
                    });
                } catch (error) {
                    console.warn(`  Warning: Could not process ${file}:`, error.message);
                }
            }
        }

        this.areaOfInterestBounds = combinedBounds;
        console.log(`Area bounds loaded: [${combinedBounds ? combinedBounds.map(n => n.toFixed(3)).join(', ') : 'No bounds'}]`);
    }

    isWithinAreaBounds(longitude, latitude) {
        if (!this.areaOfInterestBounds) return true;
        
        return longitude >= this.areaOfInterestBounds[0] && 
               longitude <= this.areaOfInterestBounds[2] &&
               latitude >= this.areaOfInterestBounds[1] && 
               latitude <= this.areaOfInterestBounds[3];
    }

    boundsIntersectArea(bounds) {
        if (!bounds || !this.areaOfInterestBounds) return true;
        
        return !(bounds[2] < this.areaOfInterestBounds[0] || 
                 bounds[0] > this.areaOfInterestBounds[2] ||
                 bounds[3] < this.areaOfInterestBounds[1] || 
                 bounds[1] > this.areaOfInterestBounds[3]);
    }

    async clearExistingData() {
        console.log('Clearing existing data...');
        const tables = [
            'co2_sources', 'voting_districts', 'landfills', 'gravel_pits',
            'wastewater_plants', 'gas_pipelines', 'gas_storage_sites',
            'gas_distribution_points', 'compressor_stations', 'study_area_boundaries',
            'groundwater_protection', 'conservation_areas', 'settlement_areas',
            'highways', 'railways'
        ];

        for (const table of tables) {
            try {
                await this.client.query(`TRUNCATE TABLE ${table} CASCADE`);
            } catch (error) {
                console.warn(`Could not truncate ${table}:`, error.message);
            }
        }
    }

    async importBoundaries() {
        console.log('Importing study area boundaries...');
        const boundaryFile = path.join(this.shapefileDir, 'sal_aus_communes.shp');
        
        if (fs.existsSync(boundaryFile)) {
            const features = [];
            await shapefile.read(boundaryFile).then(collection => {
                collection.features.forEach(feature => features.push(feature));
            });

            let processed = 0;
            for (const feature of features) {
                const properties = feature.properties;
                const geometry = this.transformGeometry(feature.geometry);
                if (!this.isValidGeoJSON(geometry)) continue;

                await this.client.query(`
                    INSERT INTO study_area_boundaries (g_id, g_name, geom, properties)
                    VALUES ($1, $2, ST_SetSRID(ST_GeomFromGeoJSON($3), 4326), $4)
                    ON CONFLICT (g_id) DO UPDATE SET geom = EXCLUDED.geom
                `, [
                    properties.g_id || properties.G_ID,
                    properties.g_name || properties.G_NAME,
                    JSON.stringify(geometry),
                    JSON.stringify(properties)
                ]);
                processed++;
            }
            console.log(`Imported ${processed} boundaries`);
        } else {
            console.log('Boundary file not found, skipping...');
        }
    }

    // UPDATED: Import voting districts directly from updated shapefile with correct column mapping
    async importUpdatedVotingDistricts() {
        console.log('Importing voting districts from updated shapefile...');
        const votingFile = path.join(this.shapefileDir, 'updated_commune.shp');
        
        if (!fs.existsSync(votingFile)) {
            console.log('Updated commune shapefile not found, skipping voting districts...');
            return;
        }

        const features = [];
        await shapefile.read(votingFile).then(collection => {
            collection.features.forEach(feature => features.push(feature));
        });

        let processed = 0;
        let validGeometry = 0;
        let districtsWithData = 0;
        let districtsWithoutData = 0;

        for (const feature of features) {
            const properties = feature.properties;
            console.log(Object.keys(properties));
            const geometry = this.transformGeometry(feature.geometry);

            // 🔎 Debug log: check raw properties coming from shapefile
            console.log("District:", properties.g_name), {
                "SPÖ?": properties["SPÃ–_perce"],
                "ÖVP?": properties["Ã–VP_perce"],
                "FPÖ?": properties["FPÃ–_perce"],
                "Grüne?": properties["GRÃœNE_per"],
                "KPÖ?": properties["KPÃ–_perce"],
                "NEOS?": properties["NEOS_perce"]
            }
            
            processed++;
            
            if (!this.isValidGeoJSON(geometry)) {
                console.warn(`Invalid geometry for district: ${properties.g_name || properties.name}`);
                continue;
            }

            validGeometry++;

            function getProp(properties, keys){
                // normalize props keys to ASCII-friendly versions
                const normalized = {};
                for (const [k, v] of Object.entries(properties)) {
                    normalized[k
                        .replace(/Ã–/g, "OE")
                        .replace(/Ãœ/g, "UE")
                        .replace(/Ã„/g, "AE")
                        .replace(/Ã¶/g, "oe")
                        .replace(/Ã¼/g, "ue")
                        .replace(/Ã¤/g, "ae")
                        .replace(/ÃŸ/g, "ss")
                        .replace(/[^A-Za-z0-9_]/g, "_")
                    ] = v;
                }
                for (const key of keys) {
                    if (normalized[key] !== undefined) {
                        return parseFloat(normalized[key]) || 0;
                    }
                }
                return 0;
            }

            // Map the correct column names from the updated shapefile
            const spoPercent   = parseFloat(properties["SPO_perc"]) || 0;
            const grunePercent = parseFloat(properties["GRUENE_per"]) || 0;
            const kpoPercent   = parseFloat(properties["KPOE_perc"]) || 0;
            const ovpPercent   = parseFloat(properties["OEVP_perc"]) || 0;
            const fpoPercent   = parseFloat(properties["FPOE_perc"]) || 0;
            const neosPercent  = parseFloat(properties["NEOS_perce"]) || 0;
            const leftGreenCombined = spoPercent + grunePercent + kpoPercent;

            // Check if this district has voting data (any non-zero values)
            const hasVotingData = spoPercent > 0 || grunePercent > 0 || kpoPercent > 0 || 
                                 ovpPercent > 0 || fpoPercent > 0 || neosPercent > 0;

            if (hasVotingData) {
                districtsWithData++;
            } else {
                districtsWithoutData++;
            }

            await this.client.query(`
                INSERT INTO voting_districts (
                    gkz, name, spo_percent, ovp_percent, fpo_percent,
                    grune_percent, kpo_percent, neos_percent,
                    left_green_combined, choropleth_color, geom, properties,
                    has_voting_data
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
                    ST_SetSRID(ST_GeomFromGeoJSON($11), 4326), $12, $13)
                ON CONFLICT (gkz) DO UPDATE SET
                    name = EXCLUDED.name,
                    spo_percent = EXCLUDED.spo_percent,
                    ovp_percent = EXCLUDED.ovp_percent,
                    fpo_percent = EXCLUDED.fpo_percent,
                    grune_percent = EXCLUDED.grune_percent,
                    kpo_percent = EXCLUDED.kpo_percent,
                    neos_percent = EXCLUDED.neos_percent,
                    left_green_combined = EXCLUDED.left_green_combined,
                    choropleth_color = EXCLUDED.choropleth_color,
                    geom = EXCLUDED.geom,
                    properties = EXCLUDED.properties,
                    has_voting_data = EXCLUDED.has_voting_data
            `, [
                parseInt(properties.gkz) || null,
                properties.g_name || properties.name || 'Unknown District',
                spoPercent,
                ovpPercent,
                fpoPercent,
                grunePercent,
                kpoPercent,
                neosPercent,
                leftGreenCombined,
                hasVotingData ? this.getVotingColor(leftGreenCombined) : '#cccccc',
                JSON.stringify(geometry),
                JSON.stringify({
                    ...properties,
                    // Store original column names for reference and debugging
                    original_columns: {
                        spo_votes: properties['SPÖ_votes'],
                        spo_percent: properties['SPÖ_perce'],
                        ovp_votes: properties['ÖVP_votes'], 
                        ovp_percent: properties['ÖVP_perce'],
                        fpo_votes: properties['FPÖ_votes'],
                        fpo_percent: properties['FPÖ_perce'],
                        grune_votes: properties['GRÜNE_vot'],
                        grune_percent: properties['GRÜNE_per'],
                        kpo_votes: properties['KPÖ_votes'],
                        kpo_percent: properties['KPÖ_perce'],
                        neos_votes: properties['NEOS_votes'],
                        neos_percent: properties['NEOS_perce'],
                        bier_votes: properties['BIER_votes'],
                        bier_percent: properties['BIER_perce'],
                        mfg_votes: properties['MFG_votes'],
                        mfg_percent: properties['MFG_percen'],
                        gaza_votes: properties['GAZA_votes'],
                        gaza_percent: properties['GAZA_perce']
                    }
                }),
                hasVotingData
            ]);
        }
        
        this.stats.votingDistricts = validGeometry;
        console.log(`Imported ${validGeometry} voting districts with valid geometry from ${processed} total features`);
        console.log(`  - Districts with voting data: ${districtsWithData}`);
        console.log(`  - Districts without voting data: ${districtsWithoutData}`);
    }

    async importCSVExcelData() {
        console.log('\nImporting CSV/Excel data...');
        
        await this.importCO2Sources();
        // Removed voting data import since it's now handled by shapefile
        await this.importLandfills();
        await this.importGravelPits();
        await this.importWastewaterPlants();
    }

    async importCO2Sources() {
        const filePath = path.join(this.dataDir, 'CO2 sources.xlsx');
        if (!fs.existsSync(filePath)) {
            console.log('CO2 sources file not found');
            return;
        }

        console.log('  Importing CO2 sources...');
        const workbook = XLSX.readFile(filePath);
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        const data = XLSX.utils.sheet_to_json(sheet);

        let imported = 0;
        let filtered = 0;

        for (const row of data) {
            const longitude = parseFloat(row['Longitude']);
            const latitude = parseFloat(row['Latitude']);
            
            if (!this.isValidWGS84([longitude, latitude])) continue;
            
            if (!this.isWithinAreaBounds(longitude, latitude)) {
                filtered++;
                continue;
            }

            const totalCO2 = parseFloat(row['Total_CO2_t'] || 0);
            const isProminent = totalCO2 > 50000;

            await this.client.query(`
                INSERT INTO co2_sources (
                    plant_name, plant_type, total_co2_t, fossil_co2_t,
                    biogenic_co2_t, comment, is_prominent, pin_size,
                    geom, properties
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 
                    ST_SetSRID(ST_MakePoint($9, $10), 4326), $11)
            `, [
                row['Plant Name'], row['Plant Type'], totalCO2,
                parseFloat(row['Fossil_CO2_t'] || 0),
                parseFloat(row['Biogenic_CO2_t'] || 0),
                row['Comment'] || '', isProminent,
                isProminent ? 4 : 2,
                longitude, latitude,
                JSON.stringify(row)
            ]);
            imported++;
        }
        
        this.stats.co2Sources = imported;
        this.stats.filteredOutByArea += filtered;
        console.log(`Imported ${imported} CO2 sources (${filtered} filtered out by area)`);
    }

    async importLandfills() {
        const filePath = path.join(this.dataDir, 'LandfiilsDeponien.csv');
        if (!fs.existsSync(filePath)) {
            console.log('Landfills file not found');
            return;
        }

        console.log('  Importing landfills...');
        const results = [];
        
        await new Promise((resolve, reject) => {
            fs.createReadStream(filePath)
                .pipe(csv())
                .on('data', (data) => results.push(data))
                .on('end', resolve)
                .on('error', reject);
        });

        let imported = 0;
        let filtered = 0;

        for (const row of results) {
            const x = parseFloat(row['X_Koordina']);
            const y = parseFloat(row['Y_Koordina']);
            
            if (!this.isValidWGS84([y, x])) continue;
            
            if (!this.isWithinAreaBounds(y, x)) {
                filtered++;
                continue;
            }

            await this.client.query(`
                INSERT INTO landfills (
                    company_name, location_name, district, address,
                    facility_type, geom, properties
                ) VALUES ($1, $2, $3, $4, $5, 
                    ST_SetSRID(ST_MakePoint($6, $7), 4326), $8)
            `, [
                row['Firmen_Nam'], row['Standort_N'], row['Standort_B'],
                row['Standort_S'], row['Anlagenbez'],
                y, x,
                JSON.stringify(row)
            ]);
            imported++;
        }
        
        this.stats.landfills = imported;
        this.stats.filteredOutByArea += filtered;
        console.log(`Imported ${imported} landfills (${filtered} filtered out by area)`);
    }

    async importGravelPits() {
        const filePath = path.join(this.dataDir, 'Gravel pits  stone quarries.csv.csv');
        if (!fs.existsSync(filePath)) {
            console.log('Gravel pits file not found');
            return;
        }

        console.log('  Importing gravel pits...');
        const results = [];
        
        await new Promise((resolve, reject) => {
            fs.createReadStream(filePath)
                .pipe(csv())
                .on('data', (data) => results.push(data))
                .on('end', resolve)
                .on('error', reject);
        });

        let imported = 0;
        let filtered = 0;

        for (const row of results) {
            const lng = parseFloat(row['center_lng']);
            const lat = parseFloat(row['center_lat']);
            
            if (!this.isValidWGS84([lng, lat])) continue;
            
            if (!this.isWithinAreaBounds(lng, lat)) {
                filtered++;
                continue;
            }

            await this.client.query(`
                INSERT INTO gravel_pits (name, resource, tags, geom, properties)
                VALUES ($1, $2, $3, ST_SetSRID(ST_MakePoint($4, $5), 4326), $6)
            `, [
                row['name'], row['resource'], row['tags'],
                lng, lat,
                JSON.stringify(row)
            ]);
            imported++;
        }
        
        this.stats.gravelPits = imported;
        this.stats.filteredOutByArea += filtered;
        console.log(`Imported ${imported} gravel pits (${filtered} filtered out by area)`);
    }

    async importWastewaterPlants() {
        const filePath = path.join(this.dataDir, 'Kläranlagen  Wastewater treatment plants.csv');
        if (!fs.existsSync(filePath)) {
            console.log('Wastewater plants file not found');
            return;
        }

        console.log('  Importing wastewater plants...');
        const results = [];
        
        await new Promise((resolve, reject) => {
            fs.createReadStream(filePath)
                .pipe(csv())
                .on('data', (data) => results.push(data))
                .on('end', resolve)
                .on('error', reject);
        });

        let imported = 0;
        let filtered = 0;

        for (const row of results) {
            const longitude = parseFloat(row['long']);
            const latitude = parseFloat(row['lat']);
            
            if (!this.isValidWGS84([longitude, latitude])) continue;
            
            if (!this.isWithinAreaBounds(longitude, latitude)) {
                filtered++;
                continue;
            }

            await this.client.query(`
                INSERT INTO wastewater_plants (
                    pk, label, treatment_type, capacity, geom, properties
                ) VALUES ($1, $2, $3, $4, 
                    ST_SetSRID(ST_MakePoint($5, $6), 4326), $7)
            `, [
                row['PK'], row['LABEL'], row['ABW_BEHANDLUNG'],
                parseInt(row['KAPAZITAET'] || 0),
                longitude, latitude,
                JSON.stringify(row)
            ]);
            imported++;
        }
        
        this.stats.wastewaterPlants = imported;
        this.stats.filteredOutByArea += filtered;
        console.log(`Imported ${imported} wastewater plants (${filtered} filtered out by area)`);
    }

    async importShapefileData() {
        console.log('\nImporting shapefile data...');
        await this.importGasInfrastructure();
    }

    async importGasInfrastructure() {
        const gasFiles = [
            { file: 'Gas Network Lines.shp', table: 'gas_pipelines', type: 'line' },
            { file: 'Gas Storage Facilities.shp', table: 'gas_storage_sites', type: 'point' },
            { file: 'Gas Distribution Points.shp', table: 'gas_distribution_points', type: 'point' },
            { file: 'Compressor station.shp', table: 'compressor_stations', type: 'point' }
        ];

        for (const gasFile of gasFiles) {
            const filePath = path.join(this.shapefileDir, gasFile.file);
            if (!fs.existsSync(filePath)) {
                console.log(`${gasFile.file} not found, skipping...`);
                continue;
            }

            console.log(`  Importing ${gasFile.file}...`);
            const features = [];
            await shapefile.read(filePath).then(collection => {
                collection.features.forEach(feature => features.push(feature));
            });

            let imported = 0;
            let filtered = 0;

            for (const feature of features) {
                const geometry = this.transformGeometry(feature.geometry);
                const properties = feature.properties;

                if (gasFile.type === 'line') {
                    if (!this.isValidGeoJSON(geometry)) continue;
                    
                    if (this.areaOfInterestBounds) {
                        const bounds = this.getGeometryBounds(geometry);
                        if (!this.boundsIntersectArea(bounds)) {
                            filtered++;
                            continue;
                        }
                    }

                    await this.client.query(`
                        INSERT INTO gas_pipelines (
                            name, operator, diameter, pressure_level,
                            pipeline_type, geom, properties
                        ) VALUES ($1, $2, $3, $4, $5, 
                            ST_SetSRID(ST_GeomFromGeoJSON($6), 4326), $7)
                    `, [
                        properties.Pipeline || properties.name,
                        properties.Operator, properties.Diameter,
                        properties.Pressure, properties.Type || 'Gas Pipeline',
                        JSON.stringify(geometry), JSON.stringify(properties)
                    ]);
                    this.stats.gasPipelines++;
                    imported++;
                } else {
                    const coords = this.extractCoordinates(geometry);
                    
                    if (!this.isValidWGS84(coords)) continue;
                    
                    if (!this.isWithinAreaBounds(coords[0], coords[1])) {
                        filtered++;
                        continue;
                    }

                    const tableName = gasFile.table;
                    
                    if (tableName === 'gas_storage_sites') {
                        await this.client.query(`
                            INSERT INTO gas_storage_sites (
                                name, operator, storage_type, capacity_bcm,
                                geom, properties
                            ) VALUES ($1, $2, $3, $4, 
                                ST_SetSRID(ST_MakePoint($5, $6), 4326), $7)
                        `, [
                            properties.Name, properties.Operator,
                            properties.Type, parseFloat(properties.Capacity || 0),
                            coords[0], coords[1], JSON.stringify(properties)
                        ]);
                        this.stats.gasStorage++;
                        imported++;
                    } else if (tableName === 'gas_distribution_points') {
                        await this.client.query(`
                            INSERT INTO gas_distribution_points (
                                name, type, operator, geom, properties
                            ) VALUES ($1, $2, $3, 
                                ST_SetSRID(ST_MakePoint($4, $5), 4326), $6)
                        `, [
                            properties.Name, 'Distribution Point',
                            properties.Operator, coords[0], coords[1],
                            JSON.stringify(properties)
                        ]);
                        this.stats.gasDistribution++;
                        imported++;
                    } else if (tableName === 'compressor_stations') {
                        await this.client.query(`
                            INSERT INTO compressor_stations (
                                name, operator, capacity_info, geom, properties
                            ) VALUES ($1, $2, $3, 
                                ST_SetSRID(ST_MakePoint($4, $5), 4326), $6)
                        `, [
                            properties.Name, properties.Operator,
                            properties.Capacity, coords[0], coords[1],
                            JSON.stringify(properties)
                        ]);
                        this.stats.compressorStations++;
                        imported++;
                    }
                }
            }
            
            if (filtered > 0) {
                console.log(`Imported ${imported} from ${gasFile.file} (${filtered} filtered out by area)`);
            } else {
                console.log(`Imported ${imported} from ${gasFile.file}`);
            }
            this.stats.filteredOutByArea += filtered;
        }
    }

    async importUnsuitableAreasOptimized() {
        console.log('\nImporting unsuitable areas (optimized)...');
        
        await this.importGroundwaterAreasOptimized();
        await this.importConservationAreasOptimized(); 
        await this.importResidentialAreasOptimized();
    }

    async importGroundwaterAreasOptimized() {
        const groundwaterFiles = [
            path.join(this.unsuitableDir, 'GroundWater', 'salzburg_water', 'reprojected_salzburg_water.shp'),
            path.join(this.unsuitableDir, 'GroundWater', 'upper_austria_water', 'reprojected_upper_austria_water.shp')
        ];

        for (const file of groundwaterFiles) {
            if (!fs.existsSync(file)) continue;
            
            console.log(`  Importing ${path.basename(file)} (optimized)...`);
            const startTime = Date.now();
            
            try {
                const reader = await shapefile.open(file);
                let imported = 0;
                let processed = 0;
                let skipped = 0;

                while (true) {
                    const result = await reader.read();
                    if (result.done) break;
                    
                    processed++;
                    
                    if (processed % 1000 === 0) {
                        const elapsed = (Date.now() - startTime) / 1000;
                        console.log(`    Processed ${processed} features (${imported} imported, ${skipped} skipped) - ${elapsed.toFixed(1)}s`);
                    }

                    try {
                        const geometry = this.transformGeometry(result.value.geometry);
                        if (!this.isValidGeoJSON(geometry)) {
                            skipped++;
                            continue;
                        }

                        const bbox = this.getGeometryBounds(geometry);
                        if (!this.boundsIntersectArea(bbox)) {
                            skipped++;
                            continue;
                        }

                        let simplifiedGeom = geometry;
                        if (this.getGeometryComplexity(geometry) > 1000) {
                            simplifiedGeom = this.simplifyGeometry(geometry, 0.001);
                        }

                        await this.client.query(`
                            INSERT INTO groundwater_protection (name, protection_zone, geom, properties)
                            VALUES ($1, $2, ST_SetSRID(ST_GeomFromGeoJSON($3), 4326), $4)
                        `, [
                            result.value.properties.Name || 'Groundwater Protection Area',
                            result.value.properties.Zone || 'Protected',
                            JSON.stringify(simplifiedGeom),
                            JSON.stringify(result.value.properties)
                        ]);
                        imported++;

                    } catch (error) {
                        skipped++;
                        if (processed % 10000 === 0) {
                            console.warn(`    Error processing feature ${processed}: ${error.message}`);
                        }
                    }
                }

                await reader.close();
                
                this.stats.groundwaterAreas += imported;
                this.stats.filteredOutByArea += skipped;
                
                const elapsed = (Date.now() - startTime) / 1000;
                console.log(`Completed ${path.basename(file)}: ${imported} imported, ${skipped} skipped (${elapsed.toFixed(1)}s)`);

            } catch (error) {
                console.error(`Error importing ${file}:`, error.message);
            }
        }
    }

    async importConservationAreasOptimized() {
        const conservationFiles = [
            path.join(this.unsuitableDir, 'NatureConservation', 'salzburg_nature', 'reprojected_salzburg_nature.shp')
        ];
        
        for (let i = 1; i <= 24; i++) {
            conservationFiles.push(
                path.join(this.unsuitableDir, 'NatureConservation', 'upper_austria_nature', `u${i}.shp`)
            );
        }

        for (const file of conservationFiles) {
            if (!fs.existsSync(file)) continue;
            
            console.log(`  Importing ${path.basename(file)}...`);
            const startTime = Date.now();
            
            try {
                const reader = await shapefile.open(file);
                let imported = 0;
                let processed = 0;
                let skipped = 0;

                while (true) {
                    const result = await reader.read();
                    if (result.done) break;
                    
                    processed++;

                    try {
                        const geometry = this.transformGeometry(result.value.geometry);
                        if (!this.isValidGeoJSON(geometry)) {
                            skipped++;
                            continue;
                        }

                        const bbox = this.getGeometryBounds(geometry);
                        if (!this.boundsIntersectArea(bbox)) {
                            skipped++;
                            continue;
                        }

                        let simplifiedGeom = geometry;
                        if (this.getGeometryComplexity(geometry) > 500) {
                            simplifiedGeom = this.simplifyGeometry(geometry, 0.001);
                        }

                        await this.client.query(`
                            INSERT INTO conservation_areas (
                                name, protection_level, area_type, geom, properties
                            ) VALUES ($1, $2, $3, ST_SetSRID(ST_GeomFromGeoJSON($4), 4326), $5)
                        `, [
                            result.value.properties.Name || 'Conservation Area',
                            result.value.properties.Protection || 'Protected',
                            result.value.properties.Type || 'Nature Reserve',
                            JSON.stringify(simplifiedGeom),
                            JSON.stringify(result.value.properties)
                        ]);
                        imported++;

                    } catch (error) {
                        skipped++;
                    }
                }

                await reader.close();
                
                this.stats.conservationAreas += imported;
                this.stats.filteredOutByArea += skipped;
                
                const elapsed = (Date.now() - startTime) / 1000;
                if (imported > 0) {
                    console.log(`Completed ${path.basename(file)}: ${imported} imported, ${skipped} skipped (${elapsed.toFixed(1)}s)`);
                }

            } catch (error) {
                console.error(`Error importing ${file}:`, error.message);
            }
        }
    }

    async importResidentialAreasOptimized() {
        const residentialFiles = [
            path.join(this.unsuitableDir, 'Residential', 'salzburg_residential', 'Salzburg_Residential_BAEW_Only.shp'),
            path.join(this.unsuitableDir, 'Residential', 'upper_austria_residential', 'Upper_Austria_Residential_Official.shp')
        ];

        for (const file of residentialFiles) {
            if (!fs.existsSync(file)) continue;
            
            console.log(`  Importing ${path.basename(file)}...`);
            const startTime = Date.now();
            
            try {
                const reader = await shapefile.open(file);
                let imported = 0;
                let processed = 0;
                let skipped = 0;

                while (true) {
                    const result = await reader.read();
                    if (result.done) break;
                    
                    processed++;

                    try {
                        const geometry = this.transformGeometry(result.value.geometry);
                        if (!this.isValidGeoJSON(geometry)) {
                            skipped++;
                            continue;
                        }

                        const bbox = this.getGeometryBounds(geometry);
                        if (!this.boundsIntersectArea(bbox)) {
                            skipped++;
                            continue;
                        }

                        let simplifiedGeom = geometry;
                        if (this.getGeometryComplexity(geometry) > 500) {
                            simplifiedGeom = this.simplifyGeometry(geometry, 0.001);
                        }

                        await this.client.query(`
                            INSERT INTO settlement_areas (
                                name, area_type, population, geom, properties
                            ) VALUES ($1, $2, $3, ST_SetSRID(ST_GeomFromGeoJSON($4), 4326), $5)
                        `, [
                            result.value.properties.Name || 'Residential Area',
                            'Residential',
                            parseInt(result.value.properties.Population || 0),
                            JSON.stringify(simplifiedGeom),
                            JSON.stringify(result.value.properties)
                        ]);
                        imported++;

                    } catch (error) {
                        skipped++;
                    }
                }

                await reader.close();
                
                this.stats.residentialAreas += imported;
                this.stats.filteredOutByArea += skipped;
                
                const elapsed = (Date.now() - startTime) / 1000;
                console.log(`Completed ${path.basename(file)}: ${imported} imported, ${skipped} skipped (${elapsed.toFixed(1)}s)`);

            } catch (error) {
                console.error(`Error importing ${file}:`, error.message);
            }
        }
    }

    async importTransportInfrastructure() {
        console.log('\nImporting transport infrastructure...');
        
        await this.importRoads();
        await this.importRailways();
    }

    async importRoads() {
        const roadFiles = [
            path.join(this.roadsDir, 'salzburg_roads', 'salzburg_primary_roads.shp'),
            path.join(this.roadsDir, 'upper_austria_roads', 'upper_austria_primary_roads.shp')
        ];

        for (const file of roadFiles) {
            if (!fs.existsSync(file)) continue;
            
            console.log(`  Importing ${path.basename(file)}...`);
            const features = [];
            await shapefile.read(file).then(collection => {
                collection.features.forEach(feature => features.push(feature));
            });

            let imported = 0;
            let filtered = 0;

            for (const feature of features) {
                const geometry = this.transformGeometry(feature.geometry);
                if (!this.isValidGeoJSON(geometry)) continue;

                if (this.areaOfInterestBounds) {
                    const bounds = this.getGeometryBounds(geometry);
                    if (!this.boundsIntersectArea(bounds)) {
                        filtered++;
                        continue;
                    }
                }

                await this.client.query(`
                    INSERT INTO highways (
                        name, highway_number, road_type, geom, properties
                    ) VALUES ($1, $2, $3, ST_SetSRID(ST_GeomFromGeoJSON($4), 4326), $5)
                `, [
                    feature.properties.Name || feature.properties.ref,
                    feature.properties.ref,
                    'Primary Road',
                    JSON.stringify(geometry),
                    JSON.stringify(feature.properties)
                ]);
                imported++;
            }
            
            this.stats.roads += imported;
            this.stats.filteredOutByArea += filtered;
            
            if (filtered > 0) {
                console.log(`Imported ${imported} roads from ${path.basename(file)} (${filtered} filtered out)`);
            } else {
                console.log(`Imported ${imported} roads from ${path.basename(file)}`);
            }
        }
    }

    async importRailways() {
        const railwayFiles = [
            path.join(this.railwayDir, 'salzburg_railway', 'salzburg_railway.shp'),
            path.join(this.railwayDir, 'upper_austria_railway', 'upper_austria_railway.shp')
        ];

        for (const file of railwayFiles) {
            if (!fs.existsSync(file)) continue;
            
            console.log(`  Importing ${path.basename(file)}...`);
            const features = [];
            await shapefile.read(file).then(collection => {
                collection.features.forEach(feature => features.push(feature));
            });

            let imported = 0;
            let filtered = 0;

            for (const feature of features) {
                const geometry = this.transformGeometry(feature.geometry);
                if (!this.isValidGeoJSON(geometry)) continue;

                if (this.areaOfInterestBounds) {
                    const bounds = this.getGeometryBounds(geometry);
                    if (!this.boundsIntersectArea(bounds)) {
                        filtered++;
                        continue;
                    }
                }

                await this.client.query(`
                    INSERT INTO railways (
                        name, railway_type, operator, geom, properties
                    ) VALUES ($1, $2, $3, ST_SetSRID(ST_GeomFromGeoJSON($4), 4326), $5)
                `, [
                    feature.properties.Name || 'Railway Line',
                    feature.properties.Type || 'Main Line',
                    feature.properties.Operator || 'ÖBB',
                    JSON.stringify(geometry),
                    JSON.stringify(feature.properties)
                ]);
                imported++;
            }
            
            this.stats.railways += imported;
            this.stats.filteredOutByArea += filtered;
            
            if (filtered > 0) {
                console.log(`Imported ${imported} railways from ${path.basename(file)} (${filtered} filtered out)`);
            } else {
                console.log(`Imported ${imported} railways from ${path.basename(file)}`);
            }
        }
    }

    async createMaterializedViews() {
        console.log('\nCreating materialized views...');
        
        // Since we're loading voting data directly from shapefile, no need to join
        console.log('Voting districts already have geometry from shapefile import');

        try {
            await this.client.query('DROP MATERIALIZED VIEW IF EXISTS mv_voting_choropleth');
            await this.client.query(`
                CREATE MATERIALIZED VIEW mv_voting_choropleth AS
                SELECT vd.*, 
                       CASE 
                         WHEN NOT has_voting_data THEN '#cccccc'
                         WHEN left_green_combined >= 60 THEN '#00AA00'
                         WHEN left_green_combined >= 50 THEN '#22BB22'
                         WHEN left_green_combined >= 40 THEN '#44CC44'
                         WHEN left_green_combined >= 30 THEN '#66DD66'
                         WHEN left_green_combined >= 20 THEN '#88EE88'
                         WHEN left_green_combined >= 15 THEN '#AAAA00'
                         WHEN left_green_combined >= 10 THEN '#CCCC00'
                         WHEN left_green_combined >= 5 THEN '#DDAA00'
                         WHEN left_green_combined > 0 THEN '#EE8800'
                         ELSE '#cccccc'
                       END as fill_color
                FROM voting_districts vd
                WHERE geom IS NOT NULL AND ST_IsValid(geom) = true
            `);
            console.log('Created voting choropleth materialized view with enhanced color scale');
        } catch (error) {
            console.warn('Could not create materialized view:', error.message);
        }
    }

    // Utility methods
    getGeometryBounds(geometry) {
        try {
            return turf.bbox(geometry);
        } catch (error) {
            return null;
        }
    }

    getGeometryComplexity(geometry) {
        let count = 0;
        const countCoords = (coords) => {
            if (Array.isArray(coords[0])) {
                coords.forEach(c => countCoords(c));
            } else {
                count++;
            }
        };
        countCoords(geometry.coordinates);
        return count;
    }

    simplifyGeometry(geometry, tolerance = 0.001) {
        try {
            return turf.simplify(turf.feature(geometry), {tolerance}).geometry;
        } catch (error) {
            return geometry;
        }
    }

    transformGeometry(geometry) {
        if (!geometry) return null;
        if (this.isWGS84(geometry)) return geometry;

        const transformed = JSON.parse(JSON.stringify(geometry));
        const transformCoords = (coords) => {
            if (Array.isArray(coords[0])) {
                return coords.map(c => transformCoords(c));
            }
            let result = this.tryTransform(coords, 'EPSG:31287');
            if (!this.isValidWGS84(result)) {
                result = this.tryTransform(coords, 'EPSG:31259');
            }
            return result || coords;
        };

        transformed.coordinates = transformCoords(geometry.coordinates);
        return transformed;
    }

    tryTransform(coords, fromProj) {
        try {
            const transformer = proj4(this.projections[fromProj], 'EPSG:4326');
            return transformer.forward(coords);
        } catch (e) {
            return null;
        }
    }

    isWGS84(geometry) {
        const coords = this.extractFirstCoordinate(geometry);
        return this.isValidWGS84(coords);
    }

    isValidWGS84(coords) {
        if (!Array.isArray(coords) || coords.length < 2) return false;
        return (
            typeof coords[0] === 'number' &&
            typeof coords[1] === 'number' &&
            coords[0] >= -180 && coords[0] <= 180 &&
            coords[1] >= -90 && coords[1] <= 90
        );
    }

    isValidGeoJSON(geometry) {
        if (!geometry || !geometry.type || !geometry.coordinates) return false;
        try {
            const checkCoords = (coords) => {
                if (Array.isArray(coords[0])) {
                    return coords.every(checkCoords);
                }
                return coords.length === 2 && coords.every(c => typeof c === 'number' && isFinite(c));
            };
            return checkCoords(geometry.coordinates);
        } catch (e) {
            return false;
        }
    }

    extractFirstCoordinate(geometry) {
        if (!geometry || !geometry.coordinates) return [];
        let coords = geometry.coordinates;
        while (Array.isArray(coords[0])) {
            coords = coords[0];
        }
        return coords;
    }

    extractCoordinates(geometry) {
        const coords = this.extractFirstCoordinate(geometry);
        return this.isValidWGS84(coords) ? coords : [13.5, 47.8];
    }

    getVotingColor(percentage) {
        if (!percentage || percentage <= 0) return '#cccccc';
        
        // Enhanced color scale for left+green percentage
        if (percentage >= 60) return '#00AA00';      // Dark green for very high
        else if (percentage >= 50) return '#22BB22';  // Green for high  
        else if (percentage >= 40) return '#44CC44';  // Medium green
        else if (percentage >= 30) return '#66DD66';  // Light green
        else if (percentage >= 20) return '#88EE88';  // Very light green
        else if (percentage >= 15) return '#AAAA00';  // Yellow-green
        else if (percentage >= 10) return '#CCCC00';  // Yellow
        else if (percentage >= 5) return '#DDAA00';   // Orange-yellow
        else if (percentage > 0) return '#EE8800';    // Orange
        else return '#cccccc';                        // Gray for no data
    }

    printSummary() {
        console.log('\nIMPORT SUMMARY');
        console.log('=====================================');
        console.log(`CO₂ Sources: ${this.stats.co2Sources}`);
        console.log(`Voting Districts: ${this.stats.votingDistricts}`);
        console.log(`Landfills: ${this.stats.landfills}`);
        console.log(`Gravel Pits: ${this.stats.gravelPits}`);
        console.log(`Wastewater Plants: ${this.stats.wastewaterPlants}`);
        console.log(`Gas Pipelines: ${this.stats.gasPipelines}`);
        console.log(`Gas Storage: ${this.stats.gasStorage}`);
        console.log(`Gas Distribution: ${this.stats.gasDistribution}`);
        console.log(`Compressor Stations: ${this.stats.compressorStations}`);
        console.log(`Groundwater Areas: ${this.stats.groundwaterAreas}`);
        console.log(`Conservation Areas: ${this.stats.conservationAreas}`);
        console.log(`Residential Areas: ${this.stats.residentialAreas}`);
        console.log(`Roads: ${this.stats.roads}`);
        console.log(`Railways: ${this.stats.railways}`);
        console.log(`Total Filtered by Area: ${this.stats.filteredOutByArea}`);
        console.log(`Errors: ${this.stats.errors}`);
        console.log('=====================================');
    }
}

if (require.main === module) {
    const importer = new OptimizedDataImporter();
    importer.importAllData().catch(console.error);
}

module.exports = OptimizedDataImporter;