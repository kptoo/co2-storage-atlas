import geopandas as gpd

# Path to updated shapefile
updated_path = r"C:\Users\User\OneDrive\Desktop\Upwork\co2-storage-atlas\Shapefiles\updated_commune.shp"
cleaned_path = r"C:\Users\User\OneDrive\Desktop\Upwork\co2-storage-atlas\Shapefiles\updated_commune_cleaned.shp"

# Load shapefile
gdf = gpd.read_file(updated_path)

# Mapping of problematic names → clean names
rename_map = {
    "SPÖ_votes": "SPO_votes",
    "SPÖ_perce": "SPO_perc",
    "ÖVP_votes": "OEVP_votes",
    "ÖVP_perce": "OEVP_perc",
    "FPÖ_votes": "FPOE_votes",
    "FPÖ_perce": "FPOE_perc",
    "GRÜNE_vot": "GRUENE_votes",
    "GRÜNE_per": "GRUENE_perc",
    "KPÖ_votes": "KPOE_votes",
    "KPÖ_perce": "KPOE_perc",
}

# Apply renaming
gdf = gdf.rename(columns=rename_map)

# Save to new shapefile
gdf.to_file(cleaned_path)

print("✅ Cleaned shapefile saved to:", cleaned_path)
print("📋 New columns:")
for i, col in enumerate(gdf.columns, start=1):
    print(f"{i}. {col}")
