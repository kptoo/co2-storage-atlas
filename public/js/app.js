class CO2StorageAtlas {
    constructor() {
        this.map = null;
        this.layers = {};
        this.clusterGroups = {};
        this.baseMaps = {};
        this.isAuthenticated = false;
        this.layerVisibility = {};
        this.coordinateClickMode = false;
        this.temporaryMarker = null;
        this.currentZoom = 8;
        this.performanceMode = true;
        this.authToken = null;
        this.existingSources = [];
        this.editingSource = null;
        
        // Performance tracking
        this.loadingSteps = [
            'Connecting to database...',
            'Loading CO₂ sources...',
            'Loading voting districts...',
            'Loading infrastructure...',
            'Loading transport networks...',
            'Loading unsuitable areas...',
            'Initializing clustering...',
            'Optimizing performance...',
            'Finalizing map...'
        ];
        this.currentStep = 0;
        this.layerCounts = {};
        this.featureCounts = {
            total: 0,
            visible: 0
        };

        // PNG icon configurations
        this.iconConfigs = {
            co2_sources: { path: '/icons/CO₂ Sources.png', size: [24, 24], opacity: 0.9 },
            landfills: { path: '/icons/Landfills.png', size: [20, 20], opacity: 0.8 },
            gravel_pits: { path: '/icons/Gravel Pits.png', size: [18, 18], opacity: 0.7 },
            wastewater_plants: { path: '/icons/Wastewater Plants.png', size: [20, 20], opacity: 0.6 },
            gas_storage_sites: { path: '/icons/Gas Storage.png', size: [22, 22], opacity: 0.5 },
            gas_distribution_points: { path: '/icons/Gas Distribution.png', size: [16, 16], opacity: 0.4 },
            compressor_stations: { path: '/icons/Compressor Stations.png', size: [20, 20], opacity: 0.3 }
        };
        
        this.init();
    }

    async init() {
        this.showLoading(true);
        this.updateLoadingStatus('Initializing map...', 0);
        
        try {
            this.initMap();
            this.initBaseMaps();
            this.initLayerGroups();
            this.initClusterGroups();
            this.setupEventListeners();
            
            // Check database connection first
            const dbHealthy = await this.checkDatabaseHealth();
            if (!dbHealthy) {
                throw new Error('Database connection failed');
            }
            
            await this.loadAllLayers();
            this.updateStatistics();
            this.updateDataQuality();
            this.enablePerformanceOptimizations();
            
            this.updateLoadingStatus('Complete!', 100);
            setTimeout(() => {
                this.showLoading(false);
                this.showToast('CO₂ Storage Atlas loaded successfully', 'success');
            }, 500);
            
        } catch (error) {
            console.error('Initialization failed:', error);
            this.updateLoadingStatus('Error: ' + error.message, 100);
            this.showToast('Failed to load application: ' + error.message, 'error');
            
            setTimeout(() => this.showLoading(false), 2000);
        }
    }

    initMap() {
        this.map = L.map('map', {
            preferCanvas: true,
            zoomControl: true,
            maxZoom: 18,
            minZoom: 6,
            worldCopyJump: true,
            maxBounds: [[45, 8], [50, 18]],
            zoomSnap: 1,
            wheelPxPerZoomLevel: 120,
            zoomAnimation: true,
            fadeAnimation: true,
            markerZoomAnimation: true
        }).setView([47.8, 13.5], 8);

        // Add scale control
        L.control.scale({
            position: 'bottomleft',
            metric: true,
            imperial: false,
            maxWidth: 200
        }).addTo(this.map);

        // Event handlers
        this.map.on('click', (e) => this.handleMapClick(e));
        this.map.on('zoomend', () => {
            this.currentZoom = this.map.getZoom();
            const zoomElement = document.getElementById('current-zoom');
            if (zoomElement) {
                zoomElement.textContent = this.currentZoom;
            }
            this.updateLayersForZoom();
        });

        this.map.on('moveend zoomend', () => {
            this.updateVisibleLayersCount();
            this.optimizeLayersForViewport();
        });

        console.log('✅ Map initialized');
    }

    initBaseMaps() {
        // OpenStreetMap
        this.baseMaps.osm = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
            maxZoom: 19,
            className: 'osm-tiles',
            updateWhenIdle: true,
            keepBuffer: 2
        }).addTo(this.map);

        // Satellite imagery
        this.baseMaps.satellite = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
            attribution: '© <a href="https://www.esri.com/">Esri</a>, © <a href="https://www.maxar.com/">Maxar</a>',
            maxZoom: 19,
            className: 'satellite-tiles',
            updateWhenIdle: true,
            keepBuffer: 2
        });

        // Terrain map
        this.baseMaps.terrain = L.tileLayer('https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png', {
            attribution: '© <a href="https://opentopomap.org">OpenTopoMap</a> (CC-BY-SA)',
            maxZoom: 17,
            className: 'terrain-tiles',
            updateWhenIdle: true,
            keepBuffer: 2
        });

        console.log('✅ Base maps initialized');
    }

    initLayerGroups() {
        // Primary layers (voting districts always added, CO2 sources to be checked by default)
        this.layers.votingDistricts = L.layerGroup().addTo(this.map);
        this.layers.co2Sources = L.layerGroup(); // Will be added via cluster
        
        // Infrastructure layers
        this.layers.landfills = L.layerGroup();
        this.layers.gravelPits = L.layerGroup();
        this.layers.wastewaterPlants = L.layerGroup();
        
        // Gas infrastructure
        this.layers.gasPipelines = L.layerGroup();
        this.layers.gasStorage = L.layerGroup();
        this.layers.gasDistribution = L.layerGroup();
        this.layers.compressorStations = L.layerGroup();
        
        // Transport layers
        this.layers.highways = L.layerGroup();
        this.layers.railways = L.layerGroup();
        
        // Unsuitable areas
        this.layers.groundwaterProtection = L.layerGroup();
        this.layers.conservationAreas = L.layerGroup();
        this.layers.settlementAreas = L.layerGroup();

        console.log('✅ Layer groups initialized');
    }

    initClusterGroups() {
        // Cluster configurations for different layer types
        const clusterConfigs = {
            co2Sources: {
                maxClusterRadius: 50,
                disableClusteringAtZoom: 15,
                showCoverageOnHover: false,
                animate: true,
                iconCreateFunction: function(cluster) {
                    const count = cluster.getChildCount();
                    const size = count < 10 ? 'small' : count < 100 ? 'medium' : 'large';
                    return new L.DivIcon({
                        html: `<div class="cluster-inner co2-cluster">${count}</div>`,
                        className: `cluster-marker cluster-${size}`,
                        iconSize: new L.Point(40, 40)
                    });
                }
            },
            landfills: {
                maxClusterRadius: 40,
                disableClusteringAtZoom: 13,
                iconCreateFunction: function(cluster) {
                    const count = cluster.getChildCount();
                    return new L.DivIcon({
                        html: `<div class="cluster-inner landfill-cluster">${count}</div>`,
                        className: `cluster-marker cluster-small`,
                        iconSize: new L.Point(35, 35)
                    });
                }
            },
            gravelPits: {
                maxClusterRadius: 40,
                disableClusteringAtZoom: 13,
                iconCreateFunction: function(cluster) {
                    const count = cluster.getChildCount();
                    return new L.DivIcon({
                        html: `<div class="cluster-inner gravel-cluster">${count}</div>`,
                        className: `cluster-marker cluster-small`,
                        iconSize: new L.Point(35, 35)
                    });
                }
            },
            wastewaterPlants: {
                maxClusterRadius: 45,
                disableClusteringAtZoom: 14,
                iconCreateFunction: function(cluster) {
                    const count = cluster.getChildCount();
                    return new L.DivIcon({
                        html: `<div class="cluster-inner wastewater-cluster">${count}</div>`,
                        className: `cluster-marker cluster-small`,
                        iconSize: new L.Point(35, 35)
                    });
                }
            },
            gasStorage: {
                maxClusterRadius: 60,
                disableClusteringAtZoom: 12,
                iconCreateFunction: function(cluster) {
                    const count = cluster.getChildCount();
                    return new L.DivIcon({
                        html: `<div class="cluster-inner gas-storage-cluster">${count}</div>`,
                        className: `cluster-marker cluster-small`,
                        iconSize: new L.Point(35, 35)
                    });
                }
            },
            gasDistribution: {
                maxClusterRadius: 35,
                disableClusteringAtZoom: 12,
                iconCreateFunction: function(cluster) {
                    const count = cluster.getChildCount();
                    return new L.DivIcon({
                        html: `<div class="cluster-inner gas-distribution-cluster">${count}</div>`,
                        className: `cluster-marker cluster-small`,
                        iconSize: new L.Point(30, 30)
                    });
                }
            },
            compressorStations: {
                maxClusterRadius: 50,
                disableClusteringAtZoom: 13,
                iconCreateFunction: function(cluster) {
                    const count = cluster.getChildCount();
                    return new L.DivIcon({
                        html: `<div class="cluster-inner compressor-cluster">${count}</div>`,
                        className: `cluster-marker cluster-small`,
                        iconSize: new L.Point(35, 35)
                    });
                }
            }
        };

        // Create cluster groups
        Object.keys(clusterConfigs).forEach(layerName => {
            this.clusterGroups[layerName] = L.markerClusterGroup(clusterConfigs[layerName]);
        });

        console.log('✅ Cluster groups initialized');
    }

    createCustomIcon(layerType, data = {}) {
        const config = this.iconConfigs[layerType];
        if (!config) {
            // Fallback to circle markers for layers without PNG icons
            return null;
        }

        const size = config.size;
        const prominent = data.is_prominent || data.total_co2_t > 50000;
        
        // Adjust size for prominent markers
        const adjustedSize = prominent && layerType === 'co2_sources' 
            ? [size[0] * 1.2, size[1] * 1.2] 
            : size;

        return L.icon({
            iconUrl: config.path,
            iconSize: adjustedSize,
            iconAnchor: [adjustedSize[0] / 2, adjustedSize[1]],
            popupAnchor: [0, -adjustedSize[1] + 5],
            className: `custom-marker ${layerType}-marker ${prominent ? 'prominent' : ''}`
        });
    }

    createFallbackMarker(data, color, size, opacity) {
        return L.circleMarker([data.latitude, data.longitude], {
            radius: size,
            fillColor: color,
            color: '#fff',
            weight: 2,
            opacity: 1,
            fillOpacity: opacity,
            className: 'fallback-marker'
        });
    }

    async checkDatabaseHealth() {
        try {
            const response = await fetch('/api/health');
            const data = await response.json();
            
            const dbStatus = document.getElementById('db-status');
            if (dbStatus) {
                if (data.status === 'OK') {
                    dbStatus.textContent = 'Connected';
                    dbStatus.className = 'quality-status success';
                } else {
                    dbStatus.textContent = 'Error';
                    dbStatus.className = 'quality-status error';
                    return false;
                }
            }

            this.updateAppStatus('connected', 'Connected to database');
            return true;
        } catch (error) {
            console.error('Database health check failed:', error);
            const dbStatus = document.getElementById('db-status');
            if (dbStatus) {
                dbStatus.textContent = 'Disconnected';
                dbStatus.className = 'quality-status error';
            }
            this.updateAppStatus('error', 'Database connection failed');
            return false;
        }
    }

    async loadAllLayers() {
        console.log('🔄 Loading all map layers...');
        let loadedCount = 0;
        const totalLayers = 14;
        
        const layerPromises = [
            this.loadVotingChoropleth(),
            this.loadCO2Sources(),
            this.loadLandfills(),
            this.loadGravelPits(),
            this.loadWastewaterPlants(),
            this.loadGasPipelines(),
            this.loadGasStorage(),
            this.loadGasDistribution(),
            this.loadCompressorStations(),
            this.loadGroundwaterProtection(),
            this.loadConservationAreas(),
            this.loadSettlementAreas(),
            this.loadHighways(),
            this.loadRailways()
        ];

        // Load layers with progress tracking
        for (let i = 0; i < layerPromises.length; i++) {
            try {
                await layerPromises[i];
                loadedCount++;
                const progress = Math.round((loadedCount / totalLayers) * 90); // Leave 10% for finalization
                this.updateLoadingStatus(this.loadingSteps[Math.min(i, this.loadingSteps.length - 1)], progress);
            } catch (error) {
                console.error(`Failed to load layer ${i}:`, error);
                // Continue loading other layers
            }
        }

        // Finalization steps
        this.updateLoadingStatus('Initializing clustering...', 92);
        this.initializeClusters();
        
        this.updateLoadingStatus('Optimizing performance...', 95);
        this.enablePerformanceOptimizations();

        const layersStatus = document.getElementById('layers-status');
        if (layersStatus) {
            layersStatus.textContent = `${loadedCount}/${totalLayers} loaded`;
            layersStatus.className = loadedCount === totalLayers ? 'quality-status success' : 'quality-status warning';
        }

        console.log(`✅ Loaded ${loadedCount}/${totalLayers} layers successfully`);
    }

    initializeClusters() {
        // Add CO2 sources cluster group to map by default (checked)
        if (this.clusterGroups.co2Sources && this.clusterGroups.co2Sources.getLayers().length > 0) {
            this.map.addLayer(this.clusterGroups.co2Sources);
        }

        // Update active cluster count
        this.updateActiveClusterCount();
    }

    // UPDATED: Load voting districts with proper data handling and reduced opacity
    async loadVotingChoropleth() {
        try {
            const response = await fetch('/api/voting-districts-choropleth');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            
            const data = await response.json();
            let count = 0;
            let validGeometryCount = 0;
            
            data.forEach(district => {
                count++; // Count all imported districts
                
                if (district.geometry && district.geom_valid !== false) {
                    try {
                        const geometry = JSON.parse(district.geometry);
                        validGeometryCount++; // Count districts with valid geometry
                        
                        // Use the choropleth color from database, or calculate based on voting data
                        const fillColor = district.fill_color || district.choropleth_color || this.getVotingColor(district.left_green_combined, district.has_voting_data);
                        
                        const polygon = L.geoJSON(geometry, {
                            style: {
                                fillColor: fillColor,
                                weight: 1,
                                opacity: 1,
                                color: 'white',
                                fillOpacity: 0.4  // REDUCED OPACITY so background data is visible
                            }
                        });

                        polygon.bindPopup(this.createVotingPopup(district));
                        
                        polygon.on({
                            mouseover: (e) => {
                                e.target.setStyle({
                                    weight: 2,
                                    color: '#666',
                                    fillOpacity: 0.6
                                });
                            },
                            mouseout: (e) => {
                                e.target.setStyle({
                                    weight: 1,
                                    color: 'white',
                                    fillOpacity: 0.4
                                });
                            }
                        });

                        this.layers.votingDistricts.addLayer(polygon);
                    } catch (error) {
                        console.warn('Invalid voting district geometry:', error);
                    }
                }
            });
            
            this.layerCounts.voting = validGeometryCount;
            this.updateLayerCount('voting-count', validGeometryCount);
            console.log(`✅ Loaded ${validGeometryCount} voting districts with valid geometry (${count} total imported)`);
            
            // If there's a significant difference, show a warning
            if (count - validGeometryCount > 10) {
                console.warn(`⚠️  ${count - validGeometryCount} voting districts lack valid geometry`);
            }
        } catch (error) {
            console.error('❌ Error loading voting districts:', error);
            this.layerCounts.voting = 0;
            this.updateLayerCount('voting-count', 0);
        }
    }

    async loadCO2Sources() {
        try {
            const response = await fetch('/api/co2-sources-enhanced');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            
            const data = await response.json();
            let count = 0;
            
            data.forEach(source => {
                if (source.latitude && source.longitude && source.geom_valid !== false) {
                    try {
                        const isProminent = source.is_prominent || source.total_co2_t > 50000;
                        
                        // Try to create custom PNG icon first
                        const customIcon = this.createCustomIcon('co2_sources', source);
                        let marker;
                        
                        if (customIcon) {
                            marker = L.marker([source.latitude, source.longitude], { 
                                icon: customIcon,
                                riseOnHover: true,
                                riseOffset: isProminent ? 1000 : 500
                            });
                        } else {
                            // Fallback to circle marker
                            const size = isProminent ? 12 : 8;
                            marker = this.createFallbackMarker(source, source.pin_color || '#ff4444', size, 0.9);
                        }

                        marker.bindPopup(this.createCO2Popup(source));
                        
                        // Admin edit functionality
                        if (this.isAuthenticated) {
                            marker.on('contextmenu', (e) => {
                                e.originalEvent.preventDefault();
                                this.editCO2Source(source);
                            });
                        }

                        this.clusterGroups.co2Sources.addLayer(marker);
                        count++;
                    } catch (error) {
                        console.warn('Invalid CO2 source coordinates:', error);
                    }
                }
            });
            
            this.layerCounts.co2 = count;
            this.updateLayerCount('co2-count', count);
            console.log(`✅ Loaded ${count} CO₂ sources`);
        } catch (error) {
            console.error('❌ Error loading CO₂ sources:', error);
            this.layerCounts.co2 = 0;
            this.updateLayerCount('co2-count', 0);
        }
    }

    async loadLandfills() {
        try {
            const response = await fetch('/api/landfills-enhanced');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            
            const data = await response.json();
            let count = 0;
            
            data.forEach(landfill => {
                if (landfill.latitude && landfill.longitude && landfill.geom_valid !== false) {
                    const customIcon = this.createCustomIcon('landfills', landfill);
                    let marker;
                    
                    if (customIcon) {
                        marker = L.marker([landfill.latitude, landfill.longitude], { 
                            icon: customIcon,
                            riseOnHover: true
                        });
                    } else {
                        marker = this.createFallbackMarker(landfill, landfill.pin_color || '#ff8800', 8, 0.8);
                    }

                    marker.bindPopup(this.createLandfillPopup(landfill));
                    this.clusterGroups.landfills.addLayer(marker);
                    count++;
                }
            });
            
            this.layerCounts.landfills = count;
            this.updateLayerCount('landfills-count', count);
            console.log(`✅ Loaded ${count} landfills`);
        } catch (error) {
            console.error('❌ Error loading landfills:', error);
            this.layerCounts.landfills = 0;
            this.updateLayerCount('landfills-count', 0);
        }
    }

    async loadGravelPits() {
        try {
            const response = await fetch('/api/gravel-pits-enhanced');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            
            const data = await response.json();
            let count = 0;
            
            data.forEach(pit => {
                if (pit.latitude && pit.longitude && pit.geom_valid !== false) {
                    const customIcon = this.createCustomIcon('gravel_pits', pit);
                    let marker;
                    
                    if (customIcon) {
                        marker = L.marker([pit.latitude, pit.longitude], { 
                            icon: customIcon,
                            riseOnHover: true
                        });
                    } else {
                        marker = this.createFallbackMarker(pit, pit.pin_color || '#8855aa', 6, 0.7);
                    }

                    marker.bindPopup(this.createGravelPitPopup(pit));
                    this.clusterGroups.gravelPits.addLayer(marker);
                    count++;
                }
            });
            
            this.layerCounts.gravel = count;
            this.updateLayerCount('gravel-count', count);
            console.log(`✅ Loaded ${count} gravel pits`);
        } catch (error) {
            console.error('❌ Error loading gravel pits:', error);
            this.layerCounts.gravel = 0;
            this.updateLayerCount('gravel-count', 0);
        }
    }

    async loadWastewaterPlants() {
        try {
            const response = await fetch('/api/wastewater-plants-enhanced');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            
            const data = await response.json();
            let count = 0;
            
            data.forEach(plant => {
                if (plant.latitude && plant.longitude && plant.geom_valid !== false) {
                    const customIcon = this.createCustomIcon('wastewater_plants', plant);
                    let marker;
                    
                    if (customIcon) {
                        marker = L.marker([plant.latitude, plant.longitude], { 
                            icon: customIcon,
                            riseOnHover: true
                        });
                    } else {
                        marker = this.createFallbackMarker(plant, plant.pin_color || '#3388ff', 8, 0.6);
                    }

                    marker.bindPopup(this.createWastewaterPopup(plant));
                    this.clusterGroups.wastewaterPlants.addLayer(marker);
                    count++;
                }
            });
            
            this.layerCounts.wastewater = count;
            this.updateLayerCount('wastewater-count', count);
            console.log(`✅ Loaded ${count} wastewater plants`);
        } catch (error) {
            console.error('❌ Error loading wastewater plants:', error);
            this.layerCounts.wastewater = 0;
            this.updateLayerCount('wastewater-count', 0);
        }
    }

    async loadGasStorage() {
        try {
            const response = await fetch('/api/gas-storage-sites-enhanced');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            
            const data = await response.json();
            let count = 0;
            
            data.forEach(storage => {
                if (storage.latitude && storage.longitude && storage.geom_valid !== false) {
                    const customIcon = this.createCustomIcon('gas_storage_sites', storage);
                    let marker;
                    
                    if (customIcon) {
                        marker = L.marker([storage.latitude, storage.longitude], { 
                            icon: customIcon,
                            riseOnHover: true
                        });
                    } else {
                        marker = this.createFallbackMarker(storage, storage.pin_color || '#00cc88', 10, 0.5);
                    }

                    marker.bindPopup(this.createGasStoragePopup(storage));
                    this.clusterGroups.gasStorage.addLayer(marker);
                    count++;
                }
            });
            
            this.layerCounts.storage = count;
            this.updateLayerCount('storage-count', count);
            console.log(`✅ Loaded ${count} gas storage sites`);
        } catch (error) {
            console.error('❌ Error loading gas storage sites:', error);
            this.layerCounts.storage = 0;
            this.updateLayerCount('storage-count', 0);
        }
    }

    async loadGasDistribution() {
        try {
            const response = await fetch('/api/gas-distribution-points-enhanced');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            
            const data = await response.json();
            let count = 0;
            
            data.forEach(point => {
                if (point.latitude && point.longitude && point.geom_valid !== false) {
                    const customIcon = this.createCustomIcon('gas_distribution_points', point);
                    let marker;
                    
                    if (customIcon) {
                        marker = L.marker([point.latitude, point.longitude], { 
                            icon: customIcon,
                            riseOnHover: true
                        });
                    } else {
                        marker = this.createFallbackMarker(point, point.pin_color || '#00aa44', 4, 0.4);
                    }

                    marker.bindPopup(this.createGasDistributionPopup(point));
                    this.clusterGroups.gasDistribution.addLayer(marker);
                    count++;
                }
            });
            
            this.layerCounts.distribution = count;
            this.updateLayerCount('distribution-count', count);
            console.log(`✅ Loaded ${count} gas distribution points`);
        } catch (error) {
            console.error('❌ Error loading gas distribution points:', error);
            this.layerCounts.distribution = 0;
            this.updateLayerCount('distribution-count', 0);
        }
    }

    async loadCompressorStations() {
        try {
            const response = await fetch('/api/compressor-stations-enhanced');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            
            const data = await response.json();
            let count = 0;
            
            data.forEach(station => {
                if (station.latitude && station.longitude && station.geom_valid !== false) {
                    const customIcon = this.createCustomIcon('compressor_stations', station);
                    let marker;
                    
                    if (customIcon) {
                        marker = L.marker([station.latitude, station.longitude], { 
                            icon: customIcon,
                            riseOnHover: true
                        });
                    } else {
                        marker = this.createFallbackMarker(station, station.pin_color || '#ffaa00', 8, 0.3);
                    }

                    marker.bindPopup(this.createCompressorPopup(station));
                    this.clusterGroups.compressorStations.addLayer(marker);
                    count++;
                }
            });
            
            this.layerCounts.compressor = count;
            this.updateLayerCount('compressor-count', count);
            console.log(`✅ Loaded ${count} compressor stations`);
        } catch (error) {
            console.error('❌ Error loading compressor stations:', error);
            this.layerCounts.compressor = 0;
            this.updateLayerCount('compressor-count', 0);
        }
    }

    async loadGasPipelines() {
        try {
            const response = await fetch('/api/gas-pipelines-enhanced');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            
            const data = await response.json();
            let count = 0;
            
            data.forEach(pipeline => {
                if (pipeline.geometry && pipeline.geom_valid !== false) {
                    try {
                        const geometry = JSON.parse(pipeline.geometry);
                        
                        const polyline = L.geoJSON(geometry, {
                            style: {
                                color: pipeline.line_color || '#00aa44',
                                weight: this.currentZoom > 10 ? (pipeline.line_weight || 4) : 2,
                                opacity: pipeline.line_opacity || 0.8
                            }
                        });

                        polyline.bindPopup(this.createPipelinePopup(pipeline));
                        this.layers.gasPipelines.addLayer(polyline);
                        count++;
                    } catch (error) {
                        console.warn('Invalid pipeline geometry:', error);
                    }
                }
            });
            
            this.layerCounts.pipelines = count;
            this.updateLayerCount('pipelines-count', count);
            console.log(`✅ Loaded ${count} gas pipelines`);
        } catch (error) {
            console.error('❌ Error loading gas pipelines:', error);
            this.layerCounts.pipelines = 0;
            this.updateLayerCount('pipelines-count', 0);
        }
    }

    // Load other layers (groundwater, conservation, etc.) - similar pattern
    async loadGroundwaterProtection() {
        try {
            const response = await fetch('/api/groundwater-protection');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            
            const data = await response.json();
            let count = 0;
            
            data.forEach(area => {
                if (area.geometry && area.geom_valid !== false) {
                    try {
                        const geometry = JSON.parse(area.geometry);
                        
                        const polygon = L.geoJSON(geometry, {
                            style: {
                                fillColor: area.fill_color || '#0066ff',
                                weight: this.currentZoom > 12 ? (area.border_weight || 2) : 1,
                                opacity: 1,
                                color: area.border_color || '#0044cc',
                                fillOpacity: area.fill_opacity || 0.3
                            }
                        });

                        polygon.bindPopup(`<div class="popup-content"><h4>Groundwater Protection</h4><p><strong>Name:</strong> ${area.name || 'Protected Area'}</p><p><strong>Zone:</strong> ${area.protection_zone || 'Protected'}</p></div>`);
                        this.layers.groundwaterProtection.addLayer(polygon);
                        count++;
                    } catch (error) {
                        console.warn('Invalid groundwater protection geometry:', error);
                    }
                }
            });
            
            this.layerCounts.groundwater = count;
            this.updateLayerCount('groundwater-count', count);
            console.log(`✅ Loaded ${count} groundwater protection areas`);
        } catch (error) {
            console.error('❌ Error loading groundwater protection:', error);
            this.layerCounts.groundwater = 0;
            this.updateLayerCount('groundwater-count', 0);
        }
    }

    async loadConservationAreas() {
        try {
            const response = await fetch('/api/conservation-areas');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            
            const data = await response.json();
            let count = 0;
            
            data.forEach(area => {
                if (area.geometry && area.geom_valid !== false) {
                    try {
                        const geometry = JSON.parse(area.geometry);
                        
                        const polygon = L.geoJSON(geometry, {
                            style: {
                                fillColor: area.fill_color || '#00ff00',
                                weight: this.currentZoom > 12 ? (area.border_weight || 2) : 1,
                                opacity: 1,
                                color: area.border_color || '#00cc00',
                                fillOpacity: area.fill_opacity || 0.3
                            }
                        });

                        polygon.bindPopup(`<div class="popup-content"><h4>Conservation Area</h4><p><strong>Name:</strong> ${area.name || 'Protected Area'}</p><p><strong>Type:</strong> ${area.area_type || 'Nature Reserve'}</p></div>`);
                        this.layers.conservationAreas.addLayer(polygon);
                        count++;
                    } catch (error) {
                        console.warn('Invalid conservation area geometry:', error);
                    }
                }
            });
            
            this.layerCounts.conservation = count;
            this.updateLayerCount('conservation-count', count);
            console.log(`✅ Loaded ${count} conservation areas`);
        } catch (error) {
            console.error('❌ Error loading conservation areas:', error);
            this.layerCounts.conservation = 0;
            this.updateLayerCount('conservation-count', 0);
        }
    }

    async loadSettlementAreas() {
        try {
            const response = await fetch('/api/settlement-areas');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            
            const data = await response.json();
            let count = 0;
            
            data.forEach(area => {
                if (area.geometry && area.geom_valid !== false) {
                    try {
                        const geometry = JSON.parse(area.geometry);
                        
                        const polygon = L.geoJSON(geometry, {
                            style: {
                                fillColor: area.fill_color || '#ff0000',
                                weight: this.currentZoom > 12 ? (area.border_weight || 2) : 1,
                                opacity: 1,
                                color: area.border_color || '#cc0000',
                                fillOpacity: area.fill_opacity || 0.3
                            }
                        });

                        polygon.bindPopup(`<div class="popup-content"><h4>Residential Area</h4><p><strong>Name:</strong> ${area.name || 'Settlement'}</p><p><strong>Population:</strong> ${area.population ? area.population.toLocaleString() : 'Unknown'}</p></div>`);
                        this.layers.settlementAreas.addLayer(polygon);
                        count++;
                    } catch (error) {
                        console.warn('Invalid settlement area geometry:', error);
                    }
                }
            });
            
            this.layerCounts.settlements = count;
            this.updateLayerCount('settlements-count', count);
            console.log(`✅ Loaded ${count} settlement areas`);
        } catch (error) {
            console.error('❌ Error loading settlement areas:', error);
            this.layerCounts.settlements = 0;
            this.updateLayerCount('settlements-count', 0);
        }
    }

    async loadHighways() {
        try {
            const response = await fetch('/api/highways');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            
            const data = await response.json();
            let count = 0;
            
            data.forEach(road => {
                if (road.geometry && road.geom_valid !== false) {
                    try {
                        const geometry = JSON.parse(road.geometry);
                        
                        const polyline = L.geoJSON(geometry, {
                            style: {
                                color: road.line_color || '#666666',
                                weight: this.currentZoom > 10 ? (road.line_weight || 3) : 2,
                                opacity: road.line_opacity || 0.7
                            }
                        });

                        polyline.bindPopup(`<div class="popup-content"><h4>Highway</h4><p><strong>Name:</strong> ${road.name || 'Primary Road'}</p><p><strong>Number:</strong> ${road.highway_number || 'N/A'}</p></div>`);
                        this.layers.highways.addLayer(polyline);
                        count++;
                    } catch (error) {
                        console.warn('Invalid highway geometry:', error);
                    }
                }
            });
            
            this.layerCounts.highways = count;
            this.updateLayerCount('highways-count', count);
            console.log(`✅ Loaded ${count} highways`);
        } catch (error) {
            console.error('❌ Error loading highways:', error);
            this.layerCounts.highways = 0;
            this.updateLayerCount('highways-count', 0);
        }
    }

    async loadRailways() {
        try {
            const response = await fetch('/api/railways');
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            
            const data = await response.json();
            let count = 0;
            
            data.forEach(railway => {
                if (railway.geometry && railway.geom_valid !== false) {
                    try {
                        const geometry = JSON.parse(railway.geometry);
                        
                        const polyline = L.geoJSON(geometry, {
                            style: {
                                color: railway.line_color || '#8B4513',
                                weight: this.currentZoom > 10 ? (railway.line_weight || 3) : 2,
                                opacity: railway.line_opacity || 0.8,
                                dashArray: '10, 10'
                            }
                        });

                        polyline.bindPopup(`<div class="popup-content"><h4>Railway</h4><p><strong>Name:</strong> ${railway.name || 'Railway Line'}</p><p><strong>Operator:</strong> ${railway.operator || 'N/A'}</p></div>`);
                        this.layers.railways.addLayer(polyline);
                        count++;
                    } catch (error) {
                        console.warn('Invalid railway geometry:', error);
                    }
                }
            });
            
            this.layerCounts.railways = count;
            this.updateLayerCount('railways-count', count);
            console.log(`✅ Loaded ${count} railways`);
        } catch (error) {
            console.error('❌ Error loading railways:', error);
            this.layerCounts.railways = 0;
            this.updateLayerCount('railways-count', 0);
        }
    }

    // Performance optimization methods
    enablePerformanceOptimizations() {
        // Throttle map events for better performance
        this.map.on('move', this.throttle(() => {
            this.optimizeLayersForViewport();
        }, 250));

        // Debounce zoom events
        this.map.on('zoom', this.debounce(() => {
            this.updateLayersForZoom();
        }, 300));

        // Enable request animation frame for smooth updates
        this.map.options.renderer = L.canvas({ tolerance: 5 });
    }

    updateLayersForZoom() {
        const zoom = this.currentZoom;
        
        // Adjust line weights based on zoom
        if (this.layers.gasPipelines) {
            this.layers.gasPipelines.eachLayer(layer => {
                if (layer.setStyle) {
                    layer.setStyle({ weight: zoom > 10 ? 4 : 2 });
                }
            });
        }

        if (this.layers.highways) {
            this.layers.highways.eachLayer(layer => {
                if (layer.setStyle) {
                    layer.setStyle({ weight: zoom > 10 ? 3 : 2 });
                }
            });
        }

        if (this.layers.railways) {
            this.layers.railways.eachLayer(layer => {
                if (layer.setStyle) {
                    layer.setStyle({ weight: zoom > 10 ? 3 : 2 });
                }
            });
        }

        // Adjust polygon border weights
        const polygonLayers = ['groundwaterProtection', 'conservationAreas', 'settlementAreas'];
        polygonLayers.forEach(layerName => {
            if (this.layers[layerName]) {
                this.layers[layerName].eachLayer(layer => {
                    if (layer.setStyle) {
                        layer.setStyle({ weight: zoom > 12 ? 2 : 1 });
                    }
                });
            }
        });
    }

    optimizeLayersForViewport() {
        if (!this.performanceMode) return;

        const bounds = this.map.getBounds();
        let visibleCount = 0;

        // Count visible features for statistics
        Object.values(this.layers).forEach(layerGroup => {
            layerGroup.eachLayer(layer => {
                if (layer.getBounds && bounds.intersects(layer.getBounds())) {
                    visibleCount++;
                } else if (layer.getLatLng && bounds.contains(layer.getLatLng())) {
                    visibleCount++;
                }
            });
        });

        this.featureCounts.visible = visibleCount;
    }

    // Utility functions for performance
    throttle(func, limit) {
        let inThrottle;
        return function() {
            const args = arguments;
            const context = this;
            if (!inThrottle) {
                func.apply(context, args);
                inThrottle = true;
                setTimeout(() => inThrottle = false, limit);
            }
        };
    }

    debounce(func, delay) {
        let debounceTimer;
        return function() {
            const context = this;
            const args = arguments;
            clearTimeout(debounceTimer);
            debounceTimer = setTimeout(() => func.apply(context, args), delay);
        };
    }

    // UPDATED: Voting popup with proper data handling
    createVotingPopup(district) {
        // Check if district has voting data
        const hasVotingData = district.has_voting_data !== false && (
            (district.spo_percent > 0) || 
            (district.ovp_percent > 0) || 
            (district.fpo_percent > 0) || 
            (district.grune_percent > 0) || 
            (district.kpo_percent > 0) || 
            (district.neos_percent > 0)
        );
        
        // If no voting data, show simple message
        if (!hasVotingData) {
            return `
                <div class="popup-content enhanced-popup">
                    <h4>${district.name || 'Unknown District'}</h4>
                    <p><strong>Data Not in Csv</strong></p>
                </div>
            `;
        }
        
        const leftGreen = parseFloat(district.left_green_combined) || 0;
        const spoPercent = parseFloat(district.spo_percent) || 0;
        const grunePercent = parseFloat(district.grune_percent) || 0;
        const kpoPercent = parseFloat(district.kpo_percent) || 0;
        const ovpPercent = parseFloat(district.ovp_percent) || 0;
        const fpoPercent = parseFloat(district.fpo_percent) || 0;
        const neosPercent = parseFloat(district.neos_percent) || 0;
        
        return `
            <div class="popup-content enhanced-popup">
                <h4>${district.name}</h4>
                <div class="voting-breakdown">
                    <p><strong>Left+Green Combined:</strong> ${leftGreen.toFixed(1)}%</p>
                    <div class="voting-bar">
                        <div class="left-green-bar" style="width: ${Math.min(leftGreen, 100)}%; background: ${district.fill_color || district.choropleth_color || this.getVotingColor(leftGreen, true)};"></div>
                    </div>
                    <div class="party-breakdown">
                        <p>SPÖ: ${spoPercent.toFixed(1)}%</p>
                        <p>Grüne: ${grunePercent.toFixed(1)}%</p>
                        <p>KPÖ: ${kpoPercent.toFixed(1)}%</p>
                        <p>ÖVP: ${ovpPercent.toFixed(1)}%</p>
                        <p>FPÖ: ${fpoPercent.toFixed(1)}%</p>
                        ${neosPercent > 0 ? `<p>NEOS: ${neosPercent.toFixed(1)}%</p>` : ''}
                    </div>
                </div>
            </div>
        `;
    }

    createCO2Popup(source) {
        const isProminent = source.is_prominent || source.total_co2_t > 50000;
        const totalCO2 = source.total_co2_t || 0;
        const fossilCO2 = source.fossil_co2_t || 0;
        const biogenicCO2 = source.biogenic_co2_t || 0;
        
        return `
            <div class="popup-content enhanced-popup">
                <h4>${source.plant_name} ${isProminent ? '⭐' : ''}</h4>
                <div class="co2-details">
                    <p><strong>Type:</strong> ${source.plant_type || 'N/A'}</p>
                    <p><strong>Total CO₂:</strong> ${totalCO2.toLocaleString()} t/year</p>
                    
                    <div class="co2-breakdown">
                        <div class="co2-bar-container">
                            <div class="co2-bar">
                                <div class="co2-fossil" 
                                     style="width: ${totalCO2 > 0 ? (fossilCO2 / totalCO2) * 100 : 0}%; background: #ff4444;">
                                     ${fossilCO2 > 0 ? 'Fossil' : ''}
                                </div>
                                <div class="co2-biogenic" 
                                     style="width: ${totalCO2 > 0 ? (biogenicCO2 / totalCO2) * 100 : 0}%; background: #44ff44;">
                                     ${biogenicCO2 > 0 ? 'Bio' : ''}
                                </div>
                            </div>
                        </div>
                        <p><strong>Fossil:</strong> ${fossilCO2.toLocaleString()} t/year</p>
                        <p><strong>Biogenic:</strong> ${biogenicCO2.toLocaleString()} t/year</p>
                    </div>
                    
                    ${source.comment ? `<p><strong>Comment:</strong> ${source.comment}</p>` : ''}
                    ${isProminent ? '<div class="prominence-badge">Major Emitter</div>' : ''}
                </div>
                ${this.isAuthenticated ? `<div class="popup-admin-controls show"><button class="btn btn-small" onclick="atlas.editCO2Source(${source.id})">Edit</button></div>` : ''}
            </div>
        `;
    }

    createLandfillPopup(landfill) {
        return `
            <div class="popup-content enhanced-popup">
                <h4>${landfill.location_name || 'Landfill'}</h4>
                <p><strong>Company:</strong> ${landfill.company_name || 'N/A'}</p>
                <p><strong>District:</strong> ${landfill.district || 'N/A'}</p>
                <p><strong>Type:</strong> ${landfill.facility_type || 'N/A'}</p>
                ${landfill.address ? `<p><strong>Address:</strong> ${landfill.address}</p>` : ''}
            </div>
        `;
    }

    createGravelPitPopup(pit) {
        return `
            <div class="popup-content enhanced-popup">
                <h4>${pit.name || 'Gravel Pit'}</h4>
                <p><strong>Resource:</strong> ${pit.resource || 'N/A'}</p>
                ${pit.tags ? `<p><strong>Tags:</strong> ${pit.tags}</p>` : ''}
            </div>
        `;
    }

    createWastewaterPopup(plant) {
        return `
            <div class="popup-content enhanced-popup">
                <h4>${plant.label || 'Wastewater Plant'}</h4>
                <p><strong>Treatment:</strong> ${plant.treatment_type || 'N/A'}</p>
                ${plant.capacity ? `<p><strong>Capacity:</strong> ${plant.capacity.toLocaleString()} PE</p>` : ''}
                ${plant.pk ? `<p><strong>ID:</strong> ${plant.pk}</p>` : ''}
            </div>
        `;
    }

    createPipelinePopup(pipeline) {
        return `
            <div class="popup-content enhanced-popup">
                <h4>${pipeline.name || 'Gas Pipeline'}</h4>
                <p><strong>Operator:</strong> ${pipeline.operator || 'N/A'}</p>
                <p><strong>Diameter:</strong> ${pipeline.diameter || 'N/A'} mm</p>
                <p><strong>Pressure:</strong> ${pipeline.pressure_level || 'N/A'}</p>
                <p><strong>Type:</strong> ${pipeline.pipeline_type || 'N/A'}</p>
            </div>
        `;
    }

    createGasStoragePopup(storage) {
        return `
            <div class="popup-content enhanced-popup">
                <h4>${storage.name || 'Gas Storage'}</h4>
                <p><strong>Operator:</strong> ${storage.operator || 'N/A'}</p>
                <p><strong>Type:</strong> ${storage.storage_type || 'N/A'}</p>
                ${storage.capacity_bcm ? `<p><strong>Capacity:</strong> ${storage.capacity_bcm} BCM</p>` : ''}
            </div>
        `;
    }

    createGasDistributionPopup(point) {
        return `
            <div class="popup-content enhanced-popup">
                <h4>${point.name || 'Gas Distribution Point'}</h4>
                <p><strong>Type:</strong> ${point.type || 'Distribution'}</p>
                ${point.operator ? `<p><strong>Operator:</strong> ${point.operator}</p>` : ''}
            </div>
        `;
    }

    createCompressorPopup(station) {
        return `
            <div class="popup-content enhanced-popup">
                <h4>${station.name || 'Compressor Station'}</h4>
                ${station.operator ? `<p><strong>Operator:</strong> ${station.operator}</p>` : ''}
                ${station.capacity_info ? `<p><strong>Capacity:</strong> ${station.capacity_info}</p>` : ''}
            </div>
        `;
    }

    // FIXED: Event handling and UI methods - Removed duplicate panel toggle listener
    setupEventListeners() {
        // Layer toggles
        document.querySelectorAll('input[type="checkbox"][id^="layer-"]').forEach(checkbox => {
            checkbox.addEventListener('change', (e) => this.toggleLayer(e));
        });

        // Basemap selector
        const basemapSelector = document.getElementById('basemap-selector');
        if (basemapSelector) {
            basemapSelector.addEventListener('change', (e) => this.switchBasemap(e.target.value));
        }

        // Admin authentication
        const adminLoginBtn = document.getElementById('admin-login-btn');
        if (adminLoginBtn) {
            adminLoginBtn.addEventListener('click', () => this.authenticate());
        }

        const adminLogoutBtn = document.getElementById('admin-logout-btn');
        if (adminLogoutBtn) {
            adminLogoutBtn.addEventListener('click', () => this.logout());
        }

        // Layer management tools
        const showAllBtn = document.getElementById('show-all-layers');
        if (showAllBtn) {
            showAllBtn.addEventListener('click', () => this.showAllLayers());
        }

        const hideAllBtn = document.getElementById('hide-all-layers');
        if (hideAllBtn) {
            hideAllBtn.addEventListener('click', () => this.hideAllLayers());
        }

        const refreshBtn = document.getElementById('refresh-data');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => this.refreshData());
        }

        // REMOVED: Panel toggle listener (now handled in HTML inline script to avoid conflicts)
        // The panel toggle is now handled in the HTML file to prevent duplicate event listeners

        // Admin form handlers
        this.setupAdminFormHandlers();
    }

    // FIXED: Control panel toggle method with better state management
    toggleControlPanel() {
        const controlPanel = document.getElementById('control-panel');
        const panelToggle = document.getElementById('panel-toggle');
        const toggleIcon = panelToggle.querySelector('.toggle-icon');

        if (!controlPanel) return;

        const isCollapsed = controlPanel.classList.contains('collapsed');
        
        if (isCollapsed) {
            // Expand panel
            controlPanel.classList.remove('collapsed');
            if (toggleIcon) toggleIcon.textContent = '−';
            console.log('Panel expanded');
        } else {
            // Collapse panel
            controlPanel.classList.add('collapsed');
            if (toggleIcon) toggleIcon.textContent = '+';
            console.log('Panel collapsed');
        }
        
        // Save preference to localStorage
        localStorage.setItem('controlPanelCollapsed', controlPanel.classList.contains('collapsed'));
    }

    setupAdminFormHandlers() {
        // CO2 source form
        const saveCO2Btn = document.getElementById('save-co2');
        if (saveCO2Btn) {
            saveCO2Btn.addEventListener('click', () => this.saveCO2Source());
        }

        const clearCO2Btn = document.getElementById('clear-co2');
        if (clearCO2Btn) {
            clearCO2Btn.addEventListener('click', () => this.clearCO2Form());
        }

        const deleteCO2Btn = document.getElementById('delete-co2');
        if (deleteCO2Btn) {
            deleteCO2Btn.addEventListener('click', () => this.deleteCO2Source());
        }

        const getCoordinatesBtn = document.getElementById('get-coordinates');
        if (getCoordinatesBtn) {
            getCoordinatesBtn.addEventListener('click', () => this.enableCoordinateSelection());
        }

        const validateCoordinatesBtn = document.getElementById('validate-coordinates');
        if (validateCoordinatesBtn) {
            validateCoordinatesBtn.addEventListener('click', () => this.validateCoordinates());
        }

        // Database management
        const refreshStatsBtn = document.getElementById('refresh-stats');
        if (refreshStatsBtn) {
            refreshStatsBtn.addEventListener('click', () => this.refreshDatabaseStats());
        }
    }

    toggleLayer(event) {
        const layerId = event.target.id.replace('layer-', '').replace(/-/g, '');
        const layerMap = {
            'voting': 'votingDistricts',
            'co2sources': 'co2Sources',
            'landfills': 'landfills',
            'gravelpits': 'gravelPits',
            'wastewater': 'wastewaterPlants',
            'gaspipelines': 'gasPipelines',
            'gasstorage': 'gasStorage',
            'gasdistribution': 'gasDistribution',
            'compressorstations': 'compressorStations',
            'highways': 'highways',
            'railways': 'railways',
            'groundwater': 'groundwaterProtection',
            'conservation': 'conservationAreas',
            'settlements': 'settlementAreas'
        };

        const layerKey = layerMap[layerId];
        if (!layerKey) return;

        // Handle clustered layers
        const clusterGroupKey = layerKey;
        if (this.clusterGroups[clusterGroupKey]) {
            if (event.target.checked) {
                this.map.addLayer(this.clusterGroups[clusterGroupKey]);
            } else {
                this.map.removeLayer(this.clusterGroups[clusterGroupKey]);
            }
        }
        // Handle regular layers
        else if (this.layers[layerKey]) {
            if (event.target.checked) {
                this.map.addLayer(this.layers[layerKey]);
            } else {
                this.map.removeLayer(this.layers[layerKey]);
            }
        }
        
        this.updateVisibleLayersCount();
        this.updateActiveClusterCount();
    }

    switchBasemap(basemapKey) {
        // Remove all base maps
        Object.values(this.baseMaps).forEach(layer => {
            this.map.removeLayer(layer);
        });
        
        // Add selected base map
        if (this.baseMaps[basemapKey]) {
            this.baseMaps[basemapKey].addTo(this.map);
            
            // Apply appropriate class to body for styling
            document.body.className = document.body.className.replace(/\b\w+\-mode\b/g, '');
            document.body.classList.add(basemapKey + '-mode');
        }
    }

    showAllLayers() {
        document.querySelectorAll('input[type="checkbox"][id^="layer-"]').forEach(checkbox => {
            if (!checkbox.checked) {
                checkbox.checked = true;
                this.toggleLayer({ target: checkbox });
            }
        });
        this.showToast('All layers enabled', 'info');
    }

    hideAllLayers() {
        document.querySelectorAll('input[type="checkbox"][id^="layer-"]').forEach(checkbox => {
            if (checkbox.checked && checkbox.id !== 'layer-voting') { // Keep voting districts visible
                checkbox.checked = false;
                this.toggleLayer({ target: checkbox });
            }
        });
        this.showToast('All layers disabled (except voting districts)', 'info');
    }

    toggleAllLayers() {
        const allCheckboxes = document.querySelectorAll('input[type="checkbox"][id^="layer-"]');
        const checkedCount = Array.from(allCheckboxes).filter(cb => cb.checked).length;
        
        if (checkedCount > 1) {
            this.hideAllLayers();
        } else {
            this.showAllLayers();
        }
    }

    refreshAllLayers() {
        this.refreshData();
    }

    async refreshData() {
        this.showToast('Refreshing data...', 'info');
        
        // Clear existing layers
        Object.values(this.layers).forEach(layer => layer.clearLayers());
        Object.values(this.clusterGroups).forEach(cluster => cluster.clearLayers());
        
        // Reload all data
        try {
            await this.loadAllLayers();
            this.updateStatistics();
            this.showToast('Data refreshed successfully', 'success');
        } catch (error) {
            console.error('Failed to refresh data:', error);
            this.showToast('Failed to refresh data', 'error');
        }
    }

    // Authentication methods
    authenticate() {
        const password = document.getElementById('admin-password').value;
        if (password === 'co2atlas2024') {
            this.isAuthenticated = true;
            this.showToast('Authentication successful!', 'success');
            this.enableAdminFeatures();
            this.loadExistingSources();
        } else {
            this.showToast('Invalid password', 'error');
        }
    }

    logout() {
        this.isAuthenticated = false;
        this.authToken = null;
        this.existingSources = [];
        this.editingSource = null;
        this.disableAdminFeatures();
        this.showToast('Logged out successfully', 'info');
    }

    enableAdminFeatures() {
        const authForm = document.getElementById('auth-form');
        const authStatus = document.getElementById('auth-status');
        const adminPanel = document.getElementById('admin-panel');
        
        if (authForm) authForm.style.display = 'none';
        if (authStatus) authStatus.style.display = 'block';
        if (adminPanel) adminPanel.style.display = 'block';
        
        this.updateAppStatus('admin', 'Admin mode active');
    }

    disableAdminFeatures() {
        const authForm = document.getElementById('auth-form');
        const authStatus = document.getElementById('auth-status');
        const adminPanel = document.getElementById('admin-panel');
        
        if (authForm) authForm.style.display = 'block';
        if (authStatus) authStatus.style.display = 'none';
        if (adminPanel) adminPanel.style.display = 'none';
        
        const passwordField = document.getElementById('admin-password');
        if (passwordField) passwordField.value = '';
        
        this.updateAppStatus('connected', 'Connected to database');
    }

    // Admin functionality
    async loadExistingSources() {
        if (!this.isAuthenticated) return;

        try {
            const response = await fetch('/api/admin/co2-sources');
            if (response.ok) {
                this.existingSources = await response.json();
                this.displayExistingSources();
            }
        } catch (error) {
            console.error('Failed to load existing sources:', error);
        }
    }

    displayExistingSources() {
        const sourcesList = document.getElementById('sources-list');
        if (!sourcesList) return;

        if (this.existingSources.length === 0) {
            sourcesList.innerHTML = '<p>No existing sources found.</p>';
            return;
        }

        let html = '<div class="sources-grid">';
        this.existingSources.forEach(source => {
            html += `
                <div class="source-item" onclick="atlas.editCO2Source(${source.id})">
                    <h6>${source.plant_name}</h6>
                    <p>${source.plant_type}</p>
                    <small>${source.total_co2_t?.toLocaleString() || 0} t CO₂/year</small>
                </div>
            `;
        });
        html += '</div>';
        sourcesList.innerHTML = html;
    }

    editCO2Source(sourceId) {
        const source = this.existingSources.find(s => s.id == sourceId);
        if (!source) return;

        this.editingSource = source;
        
        // Populate form
        document.getElementById('co2-id').value = source.id;
        document.getElementById('co2-plant-name').value = source.plant_name || '';
        document.getElementById('co2-plant-type').value = source.plant_type || '';
        document.getElementById('co2-total').value = source.total_co2_t || '';
        document.getElementById('co2-fossil').value = source.fossil_co2_t || '';
        document.getElementById('co2-biogenic').value = source.biogenic_co2_t || '';
        document.getElementById('co2-latitude').value = source.latitude || '';
        document.getElementById('co2-longitude').value = source.longitude || '';
        document.getElementById('co2-comment').value = source.comment || '';

        // Show delete button
        const deleteBtn = document.getElementById('delete-co2');
        if (deleteBtn) deleteBtn.style.display = 'inline-block';

        // Center map on the source
        if (source.latitude && source.longitude) {
            this.map.setView([source.latitude, source.longitude], 12);
        }

        this.showToast(`Editing: ${source.plant_name}`, 'info');
    }

    enableCoordinateSelection() {
        this.coordinateClickMode = true;
        this.showToast('Click on map to select coordinates', 'info');
        document.getElementById('coordinates-display').style.display = 'block';
    }

    handleMapClick(e) {
        if (this.coordinateClickMode) {
            const lat = e.latlng.lat.toFixed(6);
            const lng = e.latlng.lng.toFixed(6);
            
            const latField = document.getElementById('co2-latitude');
            const lngField = document.getElementById('co2-longitude');
            
            if (latField) latField.value = lat;
            if (lngField) lngField.value = lng;
            
            if (this.temporaryMarker) {
                this.map.removeLayer(this.temporaryMarker);
            }
            
            this.temporaryMarker = L.marker([lat, lng], {
                icon: L.divIcon({
                    html: '📍',
                    className: 'temp-coordinate-marker',
                    iconSize: [20, 20],
                    iconAnchor: [10, 10]
                })
            }).addTo(this.map);
            
            this.coordinateClickMode = false;
            document.getElementById('coordinates-display').style.display = 'none';
            this.showToast(`Coordinates selected: ${lat}, ${lng}`, 'success');
        }
    }

    validateCoordinates() {
        const latField = document.getElementById('co2-latitude');
        const lngField = document.getElementById('co2-longitude');
        
        if (!latField || !lngField) return;
        
        const lat = parseFloat(latField.value);
        const lng = parseFloat(lngField.value);
        
        if (isNaN(lat) || isNaN(lng)) {
            this.showToast('Please enter valid coordinates', 'error');
            return;
        }
        
        if (lat < 46 || lat > 49 || lng < 9 || lng > 17) {
            this.showToast('Coordinates outside Austria region', 'warning');
            return;
        }
        
        this.map.setView([lat, lng], 12);
        this.showToast('Coordinates are valid', 'success');
    }

    async saveCO2Source() {
        const formData = {
            plant_name: document.getElementById('co2-plant-name')?.value,
            plant_type: document.getElementById('co2-plant-type')?.value,
            total_co2_t: parseFloat(document.getElementById('co2-total')?.value) || 0,
            fossil_co2_t: parseFloat(document.getElementById('co2-fossil')?.value) || 0,
            biogenic_co2_t: parseFloat(document.getElementById('co2-biogenic')?.value) || 0,
            latitude: parseFloat(document.getElementById('co2-latitude')?.value),
            longitude: parseFloat(document.getElementById('co2-longitude')?.value),
            comment: document.getElementById('co2-comment')?.value || ''
        };
        
        if (!formData.plant_name || !formData.plant_type || isNaN(formData.latitude) || isNaN(formData.longitude)) {
            this.showToast('Please fill in all required fields', 'error');
            return;
        }

        const sourceId = document.getElementById('co2-id')?.value;
        const isUpdate = sourceId && sourceId !== '';
        
        try {
            const url = isUpdate ? `/api/admin/co2-sources/${sourceId}` : '/api/admin/co2-sources';
            const method = isUpdate ? 'PUT' : 'POST';
            
            const response = await fetch(url, {
                method: method,
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(formData)
            });
            
            if (response.ok) {
                this.showToast(`CO₂ source ${isUpdate ? 'updated' : 'created'} successfully`, 'success');
                this.clearCO2Form();
                this.loadExistingSources();
                // Reload CO2 sources on map
                this.clusterGroups.co2Sources.clearLayers();
                await this.loadCO2Sources();
            } else {
                throw new Error(`Failed to ${isUpdate ? 'update' : 'create'} CO₂ source`);
            }
        } catch (error) {
            console.error('Error saving CO₂ source:', error);
            this.showToast('Failed to save CO₂ source', 'error');
        }
    }

    async deleteCO2Source() {
        const sourceId = document.getElementById('co2-id')?.value;
        if (!sourceId || !this.editingSource) {
            this.showToast('No source selected for deletion', 'error');
            return;
        }

        if (!confirm(`Are you sure you want to delete "${this.editingSource.plant_name}"?`)) {
            return;
        }

        try {
            const response = await fetch(`/api/admin/co2-sources/${sourceId}`, {
                method: 'DELETE'
            });
            
            if (response.ok) {
                this.showToast('CO₂ source deleted successfully', 'success');
                this.clearCO2Form();
                this.loadExistingSources();
                // Reload CO2 sources on map
                this.clusterGroups.co2Sources.clearLayers();
                await this.loadCO2Sources();
            } else {
                throw new Error('Failed to delete CO₂ source');
            }
        } catch (error) {
            console.error('Error deleting CO₂ source:', error);
            this.showToast('Failed to delete CO₂ source', 'error');
        }
    }

    clearCO2Form() {
        const fields = ['co2-id', 'co2-plant-name', 'co2-plant-type', 'co2-total', 'co2-fossil', 'co2-biogenic', 'co2-latitude', 'co2-longitude', 'co2-comment'];
        fields.forEach(fieldId => {
            const field = document.getElementById(fieldId);
            if (field) field.value = '';
        });
        
        // Hide delete button
        const deleteBtn = document.getElementById('delete-co2');
        if (deleteBtn) deleteBtn.style.display = 'none';
        
        this.editingSource = null;
        
        if (this.temporaryMarker) {
            this.map.removeLayer(this.temporaryMarker);
            this.temporaryMarker = null;
        }
    }

    async refreshDatabaseStats() {
        try {
            const response = await fetch('/api/database-stats');
            const stats = await response.json();
            
            const statsContainer = document.getElementById('database-stats');
            if (statsContainer) {
                let html = '<h5>Database Statistics</h5>';
                Object.entries(stats).forEach(([table, data]) => {
                    html += `<p><strong>${table.replace(/_/g, ' ')}:</strong> ${data.total} total, ${data.validGeometry} with valid geometry</p>`;
                });
                statsContainer.innerHTML = html;
            }
        } catch (error) {
            console.error('Failed to load database stats:', error);
            this.showToast('Failed to load database statistics', 'error');
        }
    }

    async optimizeDatabase() {
        if (!this.isAuthenticated) return;

        this.showToast('Optimizing database...', 'info');
        
        try {
            const response = await fetch('/api/admin/optimize-database', {
                method: 'POST'
            });
            
            if (response.ok) {
                const result = await response.json();
                this.showToast('Database optimization completed', 'success');
                
                const resultsContainer = document.getElementById('optimization-results');
                if (resultsContainer) {
                    const resultsContent = resultsContainer.querySelector('.results-content');
                    if (resultsContent) {
                        resultsContent.innerHTML = result.optimizations.map(opt => `<p>${opt}</p>`).join('');
                    }
                }
            } else {
                throw new Error('Optimization failed');
            }
        } catch (error) {
            console.error('Database optimization failed:', error);
            this.showToast('Database optimization failed', 'error');
        }
    }

    // Utility methods
    updateLayerCount(elementId, count) {
        const element = document.getElementById(elementId);
        if (element) {
            element.textContent = `(${count})`;
        }
    }

    updateStatistics() {
        const totalFeatures = Object.values(this.layerCounts).reduce((sum, count) => sum + count, 0);
        this.featureCounts.total = totalFeatures;
        
        const totalElement = document.getElementById('total-features');
        if (totalElement) {
            totalElement.textContent = totalFeatures.toLocaleString();
        }
    }

    updateVisibleLayersCount() {
        let visibleCount = 0;
        document.querySelectorAll('input[type="checkbox"][id^="layer-"]:checked').forEach(() => {
            visibleCount++;
        });
        
        const visibleElement = document.getElementById('visible-layers');
        if (visibleElement) {
            visibleElement.textContent = visibleCount;
        }
    }

    updateActiveClusterCount() {
        let activeCount = 0;
        Object.values(this.clusterGroups).forEach(cluster => {
            if (this.map.hasLayer(cluster)) {
                activeCount++;
            }
        });
        
        const clusterElement = document.getElementById('active-clusters');
        if (clusterElement) {
            clusterElement.textContent = activeCount;
        }
    }

    updateDataQuality() {
        const totalLayers = 14;
        const loadedLayers = Object.keys(this.layerCounts).length;
        
        const qualityElement = document.getElementById('layers-status');
        if (qualityElement) {
            const percentage = Math.round((loadedLayers / totalLayers) * 100);
            qualityElement.textContent = `${loadedLayers}/${totalLayers} loaded (${percentage}%)`;
            
            if (percentage === 100) {
                qualityElement.className = 'quality-status success';
            } else if (percentage >= 50) {
                qualityElement.className = 'quality-status warning';
            } else {
                qualityElement.className = 'quality-status error';
            }
        }
    }

    updateAppStatus(status, message) {
        // NOTE: App status removed since we removed title bar
        console.log(`App Status: ${status} - ${message}`);
    }

    updateLoadingStatus(message, progress) {
        const statusElement = document.getElementById('loading-status');
        const progressFill = document.getElementById('progress-fill');
        const progressText = document.getElementById('progress-text');
        
        if (statusElement) statusElement.textContent = message;
        if (progressFill) progressFill.style.width = progress + '%';
        if (progressText) progressText.textContent = progress + '%';
    }

    showLoading(show) {
        const loadingOverlay = document.getElementById('loading-overlay');
        if (loadingOverlay) {
            loadingOverlay.style.display = show ? 'flex' : 'none';
        }
    }

    showToast(message, type = 'info', duration = 3000) {
        const container = document.getElementById('toast-container');
        if (!container) return;
        
        const toast = document.createElement('div');
        toast.className = `toast toast-${type}`;
        toast.innerHTML = `
            <div class="toast-content">
                <span class="toast-message">${message}</span>
                <button class="toast-close">&times;</button>
            </div>
        `;
        
        container.appendChild(toast);
        
        const timeout = setTimeout(() => {
            if (toast.parentNode) {
                toast.parentNode.removeChild(toast);
            }
        }, duration);
        
        toast.querySelector('.toast-close').addEventListener('click', () => {
            clearTimeout(timeout);
            if (toast.parentNode) {
                toast.parentNode.removeChild(toast);
            }
        });
    }

    // UPDATED: Enhanced voting color function
    getVotingColor(percentage, hasData = true) {
        if (!hasData || !percentage || percentage <= 0) return '#cccccc';
        
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

    toggleClustering(enabled) {
        if (enabled) {
            console.log('Clustering enabled');
            this.showToast('Clustering enabled', 'info');
        } else {
            console.log('Clustering disabled');
            this.showToast('Clustering disabled', 'info');
        }
    }
}

// FIXED: Initialize the application when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    console.log('🌍 Initializing CO₂ Storage Atlas...');
    
    // Check if Leaflet is loaded
    if (typeof L === 'undefined') {
        console.error('Leaflet not loaded');
        return;
    }
    
    // Load clustering plugin if available
    if (typeof L.markerClusterGroup === 'undefined') {
        console.warn('Leaflet MarkerCluster plugin not loaded, clustering disabled');
    }
    
    // Initialize atlas
    window.atlas = new CO2StorageAtlas();
    
    // IMPORTANT: Restore panel state from localStorage 
    setTimeout(() => {
        const controlPanelCollapsed = localStorage.getItem('controlPanelCollapsed') === 'true';
        
        if (controlPanelCollapsed) {
            const controlPanel = document.getElementById('control-panel');
            const panelToggle = document.getElementById('panel-toggle');
            if (controlPanel && panelToggle) {
                controlPanel.classList.add('collapsed');
                const toggleIcon = panelToggle.querySelector('.toggle-icon');
                if (toggleIcon) toggleIcon.textContent = '+';
            }
        }
    }, 100);
});