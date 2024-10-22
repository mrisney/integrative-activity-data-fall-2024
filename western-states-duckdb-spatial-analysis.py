import duckdb
import json

# Connect to DuckDB and install/load the spatial extension
con = duckdb.connect('crash_analysis.duckdb')
con.install_extension('spatial')
con.load_extension('spatial')

# Path to the GeoJSON file (adjust the path as needed)
geojson_path = '/Users/marcrisney/Projects/jhu/mas/Fall2024/online-research-skills-spatial/data/osm_western_states_export.geojson'

# Create the table by reading the GeoJSON file using ST_READ if not already loaded
con.execute(f'''
CREATE TABLE IF NOT EXISTS cannabis_shops AS 
SELECT * FROM ST_READ('{geojson_path}');
''')

# Verify the number of records loaded
shop_count = con.execute('SELECT COUNT(*) FROM cannabis_shops').fetchone()[0]
print(f"Number of records in 'cannabis_shops' table: {shop_count}")

# Load the accident data into DuckDB and filter for California, Oregon, and Washington
con.execute('''
CREATE TABLE IF NOT EXISTS accident AS 
SELECT * FROM read_csv_auto('/Users/marcrisney/Projects/jhu/mas/Fall2024/online-research-skills-spatial/data/FARS2022NationalCSV/accident.csv', ignore_errors=true)
WHERE STATENAME IN ('California', 'Oregon', 'Washington');
''')
accident_count = con.execute('SELECT COUNT(*) FROM accident').fetchone()[0]
print(f"Number of records in 'accident' table: {accident_count}")

# Load and aggregate drugs and impairment data
con.execute('''
CREATE TABLE IF NOT EXISTS drugs_aggregated AS 
SELECT ST_CASE, COUNT(*) AS num_drugs, MAX(DRUGRES) AS primary_drug
FROM read_csv_auto('/Users/marcrisney/Projects/jhu/mas/Fall2024/online-research-skills-spatial/data/FARS2022NationalCSV/drugs.csv', ignore_errors=true)
GROUP BY ST_CASE;
''')

con.execute('''
CREATE TABLE IF NOT EXISTS drimpair_aggregated AS 
SELECT ST_CASE, COUNT(*) AS num_impairments, MAX(DRIMPAIR) AS primary_impairment
FROM read_csv_auto('/Users/marcrisney/Projects/jhu/mas/Fall2024/online-research-skills-spatial/data/FARS2022NationalCSV/drimpair.csv', ignore_errors=true)
GROUP BY ST_CASE;
''')

# Combine accident, drugs, and impairment data
con.execute('''
CREATE TABLE IF NOT EXISTS crash_combined AS
SELECT 
    a.*, 
    d.num_drugs, 
    d.primary_drug,
    i.num_impairments, 
    i.primary_impairment
FROM 
    accident a
LEFT JOIN drugs_aggregated d ON a.ST_CASE = d.ST_CASE
LEFT JOIN drimpair_aggregated i ON a.ST_CASE = i.ST_CASE;
''')

# Ensure that the crash data has spatial points using LONGITUDE and LATITUDE
con.execute('''
ALTER TABLE crash_combined ADD COLUMN IF NOT EXISTS geom GEOMETRY;
UPDATE crash_combined 
SET geom = ST_Point(LONGITUD, LATITUDE)
WHERE LONGITUD IS NOT NULL AND LATITUDE IS NOT NULL;
''')
valid_crash_geom_count = con.execute('SELECT COUNT(*) FROM crash_combined WHERE geom IS NOT NULL').fetchone()[0]
print(f"Number of valid geometries in 'crash_combined' table: {valid_crash_geom_count}")

# Export the cannabis shops as GeoJSON
cannabis_shops_geojson = con.execute('''
SELECT json_group_array(
    json_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(geom),
        'properties', json_object(
            'name', name,
            'shop_type', shop,
            'city', "addr:city",
            'state', "addr:state",
            'street', "addr:street",
            'housenumber', "addr:housenumber"
        )
    )
) AS geojson
FROM cannabis_shops
WHERE geom IS NOT NULL;
''').fetchone()[0]

# Write to GeoJSON file
with open('cannabis_shops.geojson', 'w') as f:
    f.write(f'{{"type": "FeatureCollection", "features": {cannabis_shops_geojson}}}')
print("Exported 'cannabis_shops.geojson'.")

# Export the crash_combined data as GeoJSON
crash_geojson = con.execute('''
SELECT json_group_array(
    json_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(geom),
        'properties', json_object(
            'ST_CASE', ST_CASE,
            'STATENAME', STATENAME,
            'num_drugs', num_drugs,
            'primary_drug', primary_drug,
            'num_impairments', num_impairments,
            'primary_impairment', primary_impairment
        )
    )
) AS geojson
FROM crash_combined
WHERE geom IS NOT NULL;
''').fetchone()[0]

# Write to GeoJSON file
with open('crash_combined.geojson', 'w') as f:
    f.write(f'{{"type": "FeatureCollection", "features": {crash_geojson}}}')
print("Exported 'crash_combined.geojson'.")
