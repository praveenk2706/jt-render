# Initialize BigQueryFetcher instance
from services.cloud_storage_helper import CloudStorageHelper
from services.db_helper import BigQueryFetcher, CustomQueryBuilder

ALLOWED_EXTENSIONS = set(["csv"])

UPLOAD_FOLDER = "uploads"
ALLOWED_EXTENSIONS = {"csv"}
MAX_CONTENT_LENGTH = 1 * 1024 * 1024 * 1024  # 1 GB

SCHEMA_FILE_NAME = "./view_schema.json"

# Define the cache key for storing states data
STATES_CACHE_KEY = "states_data"
# Cache key for selected states
SELECTED_STATES_CACHE_KEY = "selected-states"

# Define the cache key for storing counties data
COUNTIES_CACHE_KEY = "counties-data"
# Cache key for selected counties
SELECTED_COUNTIES_CACHE_KEY = "selected-counties"

# Define Cache Key for storing cities data
CITIES_CACHE_KEY = "cities-data"
# Cache Key for selected cities
SELECTED_CITIES_CACHE_KEY = "selected-cities"

ACCESS_TYPE_CACHE_KEY = "access-type-data"
SELECTED_ACCESS_TYPE = "select-access-type"

WELLS_CACHE_KEY = "wells-data"
SELECTED_WELLS_CACHE_KEY = "selected-wells"

TREE_COVERAGE_CACHE_KEY = "tree-coverage-data"
OWNER_TYPE_CACHE_KEY = "owner_type-data"
ACCESS_TYPE_CACHE_KEY = "access-type-data"

LOCATION_CACHE_KEY = "localtion-filter-fields"
SELECTED_LOCATION_CACHE_LEY = "selected-locations"

field_dicts = {
    "Lot Acreage": "Lot_Acreage",
    "Market Price": "Market_Price",
    "Floodzones": "Significant_Flood_Zones",
    "Wetlands": "Wetlands_Total",
    "Slopes": "slope_mean",
}


field_schema_mapping = {
    "accessType": "Access_Type",
    "significant_flood_zones": "Significant_Flood_Zones",
    "lot_acreage": "Lot_Acreage",
    "marketPrice": "Market_Price",
    "ownerType": "Owner_Type",
    "property-cities": "Property_City",
    "property-counties": "Property_County_Name",
    "property-states": "Property_State_Name",
    "slope_mean": "slope_mean",
    "treeCoverage": "tree_coverage",
    "well": "Well",
    "wetlands_total": "Wetlands_Total",
    # "zip-code-matching": "Zip_Code_Matching",
    # "Owner_do_not_mail": "Owners_Do_Not_Mail",
    "nearest_road_types": "nearest_road_type",
    "distance_to_nearest_road_from_centroid": "distance_to_nearest_road_from_centroid",
    "road_frontage": "road_frontage",
    "trees_percentage": "trees_percentage",
    "built_percentage": "built_percentage",
    "grass_percentage": "grass_percentage",
    "crops_percentage": "crops_percentage",
    "shrub_and_scrub_percentage": "shrub_and_scrub_percentage",
    "bare_percentage": "bare_percentage",
    "water_percentage": "water_percentage",
    "flooded_vegetation_percentage": "flooded_vegetation_percentage",
    "snow_and_ice_percentage": "snow_and_ice_percentage",
    "parcel_area": "parcel_area",
    "largest_rect_area": "largest_rect_area",
    "percent_rectangle": "percent_rectangle",
    "largest_square_area": "largest_square_area",
    "percent_square": "percent_square",
    "largest_rect_area_cleaned": "largest_rect_area_cleaned",
    "largest_square_area_cleaned": "largest_square_area_cleaned",
    "building_square_footage": "Building_Square_Footage",
    "clay": "Clay",
    "clay_loam": "Clay_loam",
    "coarse_sand": "Coarse_sand",
    "coarse_sandy_loam": "Coarse_sandy_loam",
    "fine_sand": "Fine_sand",
    "fine_sandy_loam": "Fine_sandy_loam",
    "loam": "Loam",
    "loamy_coarse_sand": "Loamy_coarse_sand",
    "loamy_fine_sand": "Loamy_fine_sand",
    "loamy_sand": "Loamy_sand",
    "sand": "Sand",
    "sandy_clay": "Sandy_clay",
    "sandy_clay_loam": "Sandy_clay_loam",
    "sandy_loam": "Sandy_loam",
    "silt_loam": "Silt_loam",
    "silty_clay": "Silty_clay",
    "silty_clay_loam": "Silty_clay_loam",
    "very_fine_sandy_loam": "Very_fine_sandy_loam",
    "num_buildings": "num_buildings",
    "largest_building": "largest_building",
    "smallest_building": "smallest_building",
    "percent_rectangle_area_cleaned": "percent_rectangle_area_cleaned",
    "percent_square_area_cleaned": "percent_square_area_cleaned",
    "total_building_area": "total_building_area",
    "owner-name-type": "Owner_Name_Type",
    "owner-do-not-mail": "Do_Not_Mail"
}

COUNT_ALL_QUERY_CACHED_KEY = "count-all-rows"

COUNT_DISTINCT_OWNERS_QUERY_CACHED_KEY = "count-distinct_owners"


FETCH_QUERY_CACHED = "fetch-properties-query"

bigQueryFetchInstance = BigQueryFetcher()

customQueryBuilderInstance = CustomQueryBuilder(schema_file_name=SCHEMA_FILE_NAME)


# prod
CLOUD_API_GATEWAY_URL_BASE = (
    "https://olmstead-letter-api-gateway-v2-5eyl9r4h.uc.gateway.dev"
)

CLOUD_API_GATEWAY_KEY = "AIzaSyASX1AtTsrQyC7Szhy6U9-Kx9qbu9Ja4ec"

# debug
# Export_Processor_URL = "http://localhost:8080"

# prod
# Export_Processor_URL = f"{CLOUD_API_GATEWAY_URL_BASE}/pricing-export"
Export_Processor_URL = "https://us-central1-mail-engine-411414.cloudfunctions.net/export_filtered_results_to_csv"

# debug
# PDF_GENERATOR_URL = "http://localhost:8080"

# prod
PDF_GENERATOR_URL = f"{CLOUD_API_GATEWAY_URL_BASE}/generate-pdf"


CLOUD_STORAGE_CLIENT = CloudStorageHelper(store_location="Received_From_Mail_House")
REMOTE_BUCKET_NAME_FOR_UPLOAD_CSV_FROM_MAIL_HOUSE = "olmstead-property-letters"
REMOTE_DIRECTORY_FOR_UPLOAD_CSV_FROM_MAIL_HOUSE = "Received_From_Mail_House"

NEW_REMOTE_DIRECTORY_FOR_UPLOAD_CSV_FROM_MAIL_HOUSE = "Campaign-September"
