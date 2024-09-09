import numpy as np
import pandas as pd


def load_df():
    df = pd.read_csv("services/owner_id_sorted.csv", low_memory=False)

    df.replace(["", " ", np.nan], np.nan, inplace=True)

    # Replace NaN values with 'No Information' for string columns
    float_columns = [
        "Lot_Acreage",
        "Significant_Flood_Zones",
        "slope_mean",
        "distance_to_nearest_road_from_centroid",
        "road_frontage",
        "trees_percentage",
        "built_percentage",
        "grass_percentage",
        "crops_percentage",
        "shrub_and_scrub_percentage",
        "bare_percentage",
        "water_percentage",
        "flooded_vegetation_percentage",
        "snow_and_ice_percentage",
        "parcel_area",
        "largest_rect_area",
        "percent_rectangle",
        "largest_square_area",
        "percent_square",
        "largest_rect_area_cleaned",
        "largest_square_area_cleaned",
        "Building_Square_Footage",
    ]

    for column in float_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)

    # Define columns to convert to string and handle null values
    string_columns = [
        "Owner_ID",
        "Property_City",
        "Property_State_Name",
        "Property_County_Name",
        "nearest_road_type",
        "Property_Address",
        "Unformatted_APN",
        "Alternate_APN",
        "Property_Zip_Code",
        "Property_Address_Full",
        "Legal_Description",
        "County_Land_Use",
        "Property_Subdivision",
        "Property_Section",
        "Property_Township",
        "Property_Range",
        "Property_Zoning",
        "Site_Influence",
        "APN",
        "USE_COBE",
        "USE_CO2F",
        "Clay",
        "Clay loam",
        "Coarse sand",
        "Coarse sandy loam",
        "Fine sand",
        "Fine sandy loam",
        "Loam",
        "Loamy coarse sand",
        "Loamy fine sand",
        "Loamy sand",
        "Sand",
        "Sandy clay",
        "Sandy clay loam",
        "Sandy loam",
        "Silt loam",
        "Silty clay",
        "Silty clay loam",
        "Very fine sandy loam",
        "A",
        "AE",
        "AH",
        "AREA NOT INCLUDED",
        "D",
        "VE",
        "X",
        "Estuarine and Marine Deepwater",
        "Estuarine and Marine Wetland",
        "Freshwater Emergent Wetland",
        "Freshwater Forested/Shrub Wetland",
        "Freshwater Pond",
        "Lake",
        "Riverine",
    ]

    for column in string_columns:
        if column in df.columns:
            df[column] = df[column].fillna("No Information").astype("string")

    # Convert string columns
    df = df.astype(
        {
            "Owner_ID": "string",
            "Property_City": "string",
            "Property_State_Name": "string",
            "Property_County_Name": "string",
            "nearest_road_type": "string",
        }
    )

    # Standardize casing for certain columns
    df["Property_State_Name"] = df["Property_State_Name"].str.title()
    df["Property_County_Name"] = df["Property_County_Name"].str.title()
    df["Property_City"] = df["Property_City"].str.title()
    df["nearest_road_type"] = df["nearest_road_type"].str.title()

    column_order = [
        "Owner_ID",
        "Property_State_Name",
        "Property_County_Name",
        "Property_City",
        "APN",
        "nearest_road_type",
        "Unformatted_APN",
        "Alternate_APN",
        "Property_Address",
        "Property_Zip_Code",
        "Property_Address_Full",
        "Legal_Description",
        "County_Land_Use",
        "Property_Subdivision",
        "Property_Section",
        "Property_Township",
        "Property_Range",
        "Property_Zoning",
        "APN",
        "USE_COBE",
        "USE_CO2F",
        "Clay",
        "Clay loam",
        "Coarse sand",
        "Coarse sandy loam",
        "Fine sand",
        "Fine sandy loam",
        "Loam",
        "Loamy coarse sand",
        "Loamy fine sand",
        "Loamy sand",
        "Sand",
        "Sandy clay",
        "Sandy clay loam",
        "Sandy loam",
        "Silt loam",
        "Silty clay",
        "Silty clay loam",
        "Very fine sandy loam",
        "A",
        "AE",
        "AH",
        "AREA NOT INCLUDED",
        "D",
        "VE",
        "X",
        "Estuarine and Marine Deepwater",
        "Estuarine and Marine Wetland",
        "Freshwater Emergent Wetland",
        "Freshwater Forested/Shrub Wetland",
        "Freshwater Pond",
        "Lake",
        "Riverine",
        "Lot_Acreage",
        "Significant_Flood_Zones",
        "slope_mean",
        "distance_to_nearest_road_from_centroid",
        "road_frontage",
        "trees_percentage",
        "built_percentage",
        "grass_percentage",
        "crops_percentage",
        "shrub_and_scrub_percentage",
        "bare_percentage",
        "water_percentage",
        "flooded_vegetation_percentage",
        "snow_and_ice_percentage",
        "parcel_area",
        "largest_rect_area",
        "percent_rectangle",
        "largest_square_area",
        "percent_square",
        "largest_rect_area_cleaned",
        "largest_square_area_cleaned",
        "Building_Square_Footage",
    ]

    column_order = [col for col in column_order if col in df.columns]
    df = df[column_order]
    return df


def get_filters(df):
    # Get unique states
    state_filter = df["Property_State_Name"].dropna().unique().tolist()

    # Get counties for each state
    county_filter = {}
    for state in state_filter:
        counties = (
            df[df["Property_State_Name"] == state]["Property_County_Name"]
            .dropna()
            .unique()
            .tolist()
        )
        county_filter[state] = counties

    # Get cities for each county
    city_filter = {}
    for state, counties in county_filter.items():
        for county in counties:
            cities = (
                df[df["Property_County_Name"] == county]["Property_City"]
                .dropna()
                .unique()
                .tolist()
            )
            city_filter[county] = cities

    # Get unique nearest road types
    nearest_road_type_filter = df["nearest_road_type"].dropna().unique().tolist()

    return {
        "states": state_filter,
        "counties": county_filter,
        "cities": city_filter,
        "nearest_road_types": nearest_road_type_filter,
    }


def get_filter_options(df, field):
    if df is None or not isinstance(df, pd.DataFrame):
        return None
    else:
        if df[field].dtype == "object":
            if field not in [
                "Property_State_Name",
                "Property_County_Name",
                "Property_City",
            ]:
                # Replace null, None, and NaN values with "No Information"
                df[field] = df[field].replace(
                    {
                        np.nan: "No Information",
                        None: "No Information",
                        "": "No Information",
                        "nan": "No Information",
                        "Nan": "No Information",
                        "nAn": "No Information",
                        "naN": "No Information",
                    }
                )

                # For string fields, return unique values as options
                options = df[field].unique()
                print(f"For field {field} options are {options}")
                options = set(options)

                options = list(options)
                options.sort()

                if "No Information" in options:
                    options.remove("No Information")
                    options.insert(0, "No Information")

                # options = set(options)
                return [(val, val) for val in options]
            else:
                return None

        elif df[field].dtype in ["int64", "float64"]:
            # For numeric fields, return min and max values
            return {"min": df[field].min(), "max": df[field].max()}


def apply_filters(df: pd.DataFrame, filter_dict):
    # Apply city filters
    if "property-states" in filter_dict:
        if "all" not in filter_dict["property-states"]:
            states = filter_dict["property-states"]
            df = df[df["Property_State_Name"].isin(states)]

    # Apply county filters
    if "property-counties" in filter_dict:
        if "all" not in filter_dict["property-counties"]:
            counties = filter_dict["property-counties"]
            df = df[df["Property_County_Name"].isin(counties)]

    # Apply city filters
    if "property-cities" in filter_dict:
        cities = filter_dict["property-cities"]
        if "all" not in cities:
            if "No Information" in cities:
                df = df[
                    df["Property_City"].isin(cities)
                    | df["Property_City"].isna()
                    | (df["Property_City"] == "")
                    | (df["Property_City"] == "No Information")
                ]
            else:
                df = df[
                    df["Property_City"].isin(cities)
                    | df["Property_City"].isna()
                    | (df["Property_City"] == "")
                    | (df["Property_City"] == "Nan")
                ]

    # Apply numerical filters
    # Apply lot acreage filter
    if "lot-acreage" in filter_dict:
        lot_acreage_filter = filter_dict["lot-acreage"]
        if isinstance(lot_acreage_filter, dict):
            if "min" in lot_acreage_filter.keys() and lot_acreage_filter["min"]:
                try:
                    min_val = float(lot_acreage_filter["min"])
                    df = df[df["Lot_Acreage"] >= min_val]
                except ValueError:
                    print("Invalid minimum value")

            if "max" in lot_acreage_filter.keys() and lot_acreage_filter["max"]:
                try:
                    max_val = float(lot_acreage_filter["max"])
                    df = df[df["Lot_Acreage"] <= max_val]
                except ValueError:
                    print("Invalid maximum value")

    # Apply significant flood zones filter
    if "floodzones" in filter_dict:
        print("floodzones ", filter_dict["floodzones"])
        floodzones_filter = filter_dict["floodzones"]

        if (
            floodzones_filter
            and isinstance(floodzones_filter, dict)
            and len(floodzones_filter.keys()) > 0
        ):
            if "min" in floodzones_filter.keys() and floodzones_filter["min"]:
                try:
                    min_val = float(floodzones_filter["min"])
                    df = df[df["Significant_Flood_Zones"] >= min_val]
                except ValueError:
                    print("Invalid minimum value")

            if "max" in floodzones_filter.keys() and floodzones_filter["max"]:
                try:
                    max_val = float(floodzones_filter["max"])
                    df = df[df["Significant_Flood_Zones"] <= max_val]
                except ValueError:
                    print("Invalid maximum value")

    # Apply slope mean filter
    if "slopes" in filter_dict:
        slope_mean_filter = filter_dict["slopes"]
        if isinstance(slope_mean_filter, dict):
            if "min" in slope_mean_filter.keys() and slope_mean_filter["min"]:
                try:
                    min_val = float(slope_mean_filter["min"])
                    df = df[df["slope_mean"] >= min_val]
                except ValueError:
                    print("Invalid minimum value for slope_mean")

            if "max" in slope_mean_filter.keys() and slope_mean_filter["max"]:
                try:
                    max_val = float(slope_mean_filter["max"])
                    df = df[df["slope_mean"] <= max_val]
                except ValueError:
                    print("Invalid maximum value for slope_mean")

    # Apply nearest road type filter
    if "nearest-road-type" in filter_dict:
        nearest_road_type_filter = filter_dict["nearest-road-type"]
        if (
            isinstance(nearest_road_type_filter, list)
            and "all" not in nearest_road_type_filter
        ):
            df = df[
                df["nearest_road_type"].isin(nearest_road_type_filter)
                | df["nearest_road_type"].isna()
                | (df["nearest_road_type"] == "")
                | (df["nearest_road_type"] == "No Information")
            ]

    # Apply distance to nearest road filter
    if "distance-to-nearest-road" in filter_dict:
        distance_filter = filter_dict["distance-to-nearest-road"]
        if isinstance(distance_filter, dict):
            if "min" in distance_filter.keys() and distance_filter["min"]:
                try:
                    min_val = float(distance_filter["min"])
                    df = df[df["distance_to_nearest_road_from_centroid"] >= min_val]
                except ValueError:
                    print("Invalid minimum value")

            if "max" in distance_filter.keys() and distance_filter["max"]:
                try:
                    max_val = float(distance_filter["max"])
                    df = df[df["distance_to_nearest_road_from_centroid"] <= max_val]
                except ValueError:
                    print("Invalid maximum value")

    # Apply road frontage filter
    if "road-frontage" in filter_dict:
        road_frontage_filter = filter_dict["road-frontage"]
        if isinstance(road_frontage_filter, dict):
            if "min" in road_frontage_filter.keys() and road_frontage_filter["min"]:
                try:
                    min_val = float(road_frontage_filter["min"])
                    df = df[df["road_frontage"] >= min_val]
                except ValueError:
                    print("Invalid minimum value")

            if "max" in road_frontage_filter.keys() and road_frontage_filter["max"]:
                try:
                    max_val = float(road_frontage_filter["max"])
                    df = df[df["road_frontage"] <= max_val]
                except ValueError:
                    print("Invalid maximum value")

    # Apply percentage filters
    percentage_columns = [
        "trees_percentage",
        "built_percentage",
        "grass_percentage",
        "crops_percentage",
        "shrub_and_scrub_percentage",
        "bare_percentage",
        "water_percentage",
        "flooded_vegetation_percentage",
        "snow_and_ice_percentage",
    ]

    for column in percentage_columns:
        if column in filter_dict:
            percentage_filter = filter_dict[column]
            if isinstance(percentage_filter, dict):
                if "min" in percentage_filter.keys() and percentage_filter["min"]:
                    try:
                        min_val = float(percentage_filter["min"])
                        df = df[df[column] >= min_val]
                    except ValueError:
                        print(f"Invalid minimum value for {column}")

                if "max" in percentage_filter.keys() and percentage_filter["max"]:
                    try:
                        max_val = float(percentage_filter["max"])
                        df = df[df[column] <= max_val]
                    except ValueError:
                        print(f"Invalid maximum value for {column}")

    # Apply parcel area filter
    if "parcel_area" in filter_dict:
        parcel_area_filter = filter_dict["parcel_area"]
        if isinstance(parcel_area_filter, dict):
            if "min" in parcel_area_filter.keys() and parcel_area_filter["min"]:
                try:
                    min_val = float(parcel_area_filter["min"])
                    df = df[df["parcel_area"] >= min_val]
                except ValueError:
                    print("Invalid minimum value")

            if "max" in parcel_area_filter.keys() and parcel_area_filter["max"]:
                try:
                    max_val = float(parcel_area_filter["max"])
                    df = df[df["parcel_area"] <= max_val]
                except ValueError:
                    print("Invalid maximum value")

    # Apply area filters for rectangles and squares
    area_columns = [
        "largest_rect_area",
        "percent_rectangle",
        "largest_square_area",
        "percent_square",
        "largest_rect_area_cleaned",
        "largest_square_area_cleaned",
    ]

    for column in area_columns:
        if column in filter_dict:
            area_filter = filter_dict[column]
            if isinstance(area_filter, dict):
                if "min" in area_filter.keys() and area_filter["min"]:
                    try:
                        min_val = float(area_filter["min"])
                        df = df[df[column] >= min_val]
                    except ValueError:
                        print(f"Invalid minimum value for {column}")

                if "max" in area_filter.keys() and area_filter["max"]:
                    try:
                        max_val = float(area_filter["max"])
                        df = df[df[column] <= max_val]
                    except ValueError:
                        print(f"Invalid maximum value for {column}")

    return df


def generate_filter_options(df):
    filter_options = {}

    # Mapping HTML field names to DataFrame column names
    html_to_df_mapping = {
        "state": "Property_State_Name",
        "county": "Property_County_Name",
        "city": "Property_City",
        "nearest-road-type": "nearest_road_type",
    }

    # Numeric fields for filter options
    numeric_fields = {
        "lot-acreage": "Lot_Acreage",
        "floodzones": "Significant_Flood_Zones",
        "slopes": "slope_mean",
        "distance-to-nearest-road": "distance_to_nearest_road_from_centroid",
        "road-frontage": "road_frontage",
        "trees_percentage": "trees_percentage",
        "built-percentage": "built_percentage",
        "grass-percentage": "grass_percentage",
        "crops-percentage": "crops_percentage",
        "shrub-and-scrub-percentage": "shrub_and_scrub_percentage",
        "bare-percentage": "bare_percentage",
        "water-percentage": "water_percentage",
        "flooded-vegetation-percentage": "flooded_vegetation_percentage",
        "snow-and-ice-percentage": "snow_and_ice_percentage",
        "parcel-area": "parcel_area",
        "largest-rect-area": "largest_rect_area",
        "percent-rectangle": "percent_rectangle",
        "largest-square-area": "largest_square_area",
        "percent-square": "percent_square",
        "largest-rect-area-cleaned": "largest_rect_area_cleaned",
        "largest-square-area-cleaned": "largest_square_area_cleaned",
    }

    # Process HTML field mappings
    for html_name, df_column in {**html_to_df_mapping, **numeric_fields}.items():
        filter_options[html_name] = get_filter_options(df, df_column)

    # Fetch additional filters
    data = get_filters(df)

    # Ensure data contains required keys before assigning
    filter_options["state"] = [(str(val), str(val)) for val in data.get("states", [])]
    filter_options["county"] = data.get("counties", [])
    filter_options["city"] = data.get("cities", [])
    filter_options["nearest-road-type"] = data.get("nearest_road_types", [])

    return filter_options


# def p():
#     columns = ['APN', 'Owner_First_Name_R', 'Owner_Last_Name_R', 'Owner_Full_Name_R',
#        'Property_City', 'Property_County_Name', 'Property_State_Name',
#        'flood_zone', 'tree_coverage', 'well', 'wetlands', 'Wetlands_Total',
#        'shape', 'slope_mean', 'Significant_Flood_Zones', 'Lot_Acreage',
#        'Owner_Type', 'Zip_Code_Matching', 'topography',
#        'Mail_Street_Address_R', 'Mail_Street_Address', 'Mail_City_R',
#        'Mail_City', 'Mail_State_R', 'Mail_Zip_Code_9', 'Mail_State',
#        'Market_Price', 'Unformatted_APN', 'Alternate_APN', 'Access_Type',
#        'Owner_ID']

#     col_list_name_mapping = {

#     }

#     for column in columns:
