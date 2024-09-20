import io
import os
import pathlib
import random
import sys
import tempfile
import threading
import traceback
import zipfile
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from flask import (
    Flask,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from flask_caching import Cache
from flask_cors import CORS
from werkzeug.utils import secure_filename

from global_constants import (
    ACCESS_TYPE_CACHE_KEY,
    ALLOWED_EXTENSIONS,
    CLOUD_API_GATEWAY_KEY,
    LOCATION_CACHE_KEY,
    MAX_CONTENT_LENGTH,
    NEW_REMOTE_DIRECTORY_FOR_UPLOAD_CSV_FROM_MAIL_HOUSE,
    OWNER_TYPE_CACHE_KEY,
    PDF_GENERATOR_URL,
    REMOTE_BUCKET_NAME_FOR_UPLOAD_CSV_FROM_MAIL_HOUSE,
    REMOTE_DIRECTORY_FOR_UPLOAD_CSV_FROM_MAIL_HOUSE,
    TREE_COVERAGE_CACHE_KEY,
    UPLOAD_FOLDER,
    WELLS_CACHE_KEY,
    Export_Processor_URL,
    bigQueryFetchInstance,
    customQueryBuilderInstance,
    field_dicts,
    field_schema_mapping,
)
from services.cloud_storage_helper import CloudStorageHelper

# from services.fetch_query_base import (
#     # fetch_v9_individual_query,
#     # fetch_v10_individual_query_full,
#     # fetch_v10_prefix,
#     # fetch_v11_prefix,
#     # fetch_v12_prefix,
#     # fetch_v13_prefix,
# )
from services.filterer import (  # noqa: F401
    apply_filters,
    generate_filter_options,
    load_df,
)
from services.v2_pricing_helper import PropertyRecordsPreProcessor

# print(sys.getrecursionlimit())
sys.setrecursionlimit(1000000)
# print(sys.getrecursionlimit())

app = Flask(__name__)
app.config["SECRET_KEY"] = "T3mP$7r1nGf0RFl@sk@ppS3cr3tK3y"

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

# Initialize Flask-Caching
cache = Cache(app, config={"CACHE_TYPE": "simple"})

CORS(app)

CLOUD_STORAGE_CLIENT = CloudStorageHelper(store_location="Received_From_Mail_House")

# df = load_df()


@app.route("/", methods=["GET"])
def index():
    # os.system('clear')

    # df = load_df()
    # print(df.columns)
    # filter_options = generate_filter_options(df)

    # print(filter_options) # type: ignore

    return render_template("index.html")


# @app.route("/get-filter-values", methods=["GET", "POST"])
# def get_filter_values():
#     try:
#         filter_options = generate_filter_options(df)

#         return jsonify({"message": "success", "data": filter_options})

# except Exception as e:
# print(f"Exception {e} while getting filter values")
#         traceback.print_exc()
#         return jsonify({"message": "failed", "error": str(e)})


@app.route("/get-owner-name-type", methods=["GET"])
def get_owner_name_types():
    try:
        results = bigQueryFetchInstance.runQuery(
            "SELECT DISTINCT Owner_Name_Type FROM `property-database-370200.Dataset_v3.Owners`"
        )

        if results is None or not isinstance(results, pd.DataFrame) or results.empty:
            raise Exception("Exception while fetching owner name types")

        results = list(results["Owner_Name_Type"])

        return jsonify({"message": "success", "data": results}), 200

    except Exception as e:
        return jsonify({"message": f"Error {e}"}), 500


@app.route("/get-numeric-filters", methods=["GET"])
def get_numeric_filter_values():
    try:
        results = bigQueryFetchInstance.numeric_fields_filters()

        if results is None:
            raise Exception("Exception while fetching numeric filter values")

        return jsonify({"message": "success", "data": results}), 200

    except Exception as e:
        return jsonify({"message": f"Error {e}"}), 500


@app.route("/get-filter-values", methods=["GET", "POST"])
def get_filter_values():
    try:
        # Create an instance of BigQueryFetcher
        bigquery_fetcher = bigQueryFetchInstance

        # Get filter options from BigQuery
        response = bigquery_fetcher.prop_states_counties_cities()

        if response is None or "data" not in response:
            raise Exception("Prop filter initialization failed")

        location = response["data"]
        print(location.keys())

        nearest_road_types = location["nearest_road_type"]
        del location["nearest_road_type"]

        filter_options = {
            "state": location,
            "nearest_road_types": nearest_road_types,
        }

        print(filter_options, "filter_options----------")

        return jsonify({"message": "success", "data": filter_options})

    except Exception as e:
        # print(f"Exception {e} while getting filter values")
        traceback.print_exc()
        return jsonify({"message": "failed", "error": str(e)})


@app.route("/query-db", methods=["GET"])
def query_records_page():
    return render_template("fetch_records.html")


def number_input_validator(value, field_name):
    try:
        new_val = float(value) if value else None
        return new_val
    except Exception as e:
        # print(f"exception {e} in validating {value} for field {field_name}")
        return None


# Helper function to ensure the value is a list
def ensure_list(value):
    if value is not None and not isinstance(value, list):
        if str(value).strip() != "" and "all" not in str(value).lower():
            return [
                value,
            ]
        else:
            return None
    return value


def convert_to_int_list(str_list):
    converted_list = []
    for item in str_list:
        if item is None or item.lower() == "none":
            converted_list.append(None)
        else:
            try:
                converted_list.append(int(item))
            except ValueError:
                return []  # Return an empty list in case of a ValueError
    return converted_list


@app.route("/query-count", methods=["POST"])
def query_counts_after_filter():
    try:
        request_body = request.get_json(silent=True)
        if request_body is None:
            return jsonify({"message": "No JSON data received"}), 400

        filters = {}
        null_checks = []
        outside_filters = []

        # Separate filters and null checks
        for key, value in request_body.items():
            # if key.endswith("-include-null"):
            #     field_name = key.replace("-include-null", "")
            #     if value:  # include-null is true
            #         if key == "zip-code-matching-include-null":
            #             null_checks.append(
            #                 "Property_Zip_Code IS NULL OR Mail_Zip_Code IS NULL OR "
            #                 "Mail_Zip_Code_9 IS NULL OR Mail_Zip_Code_R IS NULL OR "
            #                 "Mail_ZipCode_STD IS NULL"
            #             )
            #         else:
            #             null_checks.append(
            #                 f"{field_schema_mapping.get(field_name)} IS NULL"
            #             )
            # else:
            filters[key] = value

        # Helper function to handle range filters
        def range_filter(field_name, field_value):
            return {
                "min": number_input_validator(
                    value=field_value.get("min"), field_name=f"{field_name}-min"
                ),
                "max": number_input_validator(
                    value=field_value.get("max"), field_name=f"{field_name}-max"
                ),
            }

        # Construct the subqueries
        for key, value in filters.items():
            sub_query = None
            include_null = False  # Track if we should include NULL condition

            if key == "zip-code-matching" and value is not None:
                sub_query = customQueryBuilderInstance.build_query(
                    field_name=key,
                    filter_values=value,
                )

                # print(sub_query)
            if key in field_schema_mapping:
                mapped_field_name = field_schema_mapping[key]

                # Check if the field has an include-null flag
                include_null_key = f"{key}-include-null"
                include_null = (
                    include_null_key in request_body and request_body[include_null_key]
                )

                if isinstance(value, dict) and ("min" in value or "max" in value):
                    filter_values = range_filter(key, value)
                    sub_query = customQueryBuilderInstance.build_query(
                        field_name=mapped_field_name,
                        filter_values=filter_values,
                    )
                elif isinstance(value, list) and value:
                    sub_query = customQueryBuilderInstance.build_query(
                        field_name=mapped_field_name,
                        filter_values=value,
                    )
                elif isinstance(value, (int, float)):
                    sub_query = customQueryBuilderInstance.build_query(
                        field_name=mapped_field_name,
                        filter_values=value,
                    )
                elif isinstance(value, str) and value.strip():
                    sub_query = customQueryBuilderInstance.build_query(
                        field_name=mapped_field_name,
                        filter_values=value,
                    )

            # Add to appropriate filter list
            if sub_query:
                # If include-null is true, add the sub_query and IS NULL combined inside WITH A
                if include_null:
                    null_checks.append(f"({sub_query} OR {mapped_field_name} IS NULL)")
                else:
                    outside_filters.append(sub_query)

        # Build the final query with WITH A AS block and outside WHERE clause
        null_checks_query = " AND ".join(null_checks)
        outside_filters_query = " AND ".join(outside_filters)

        # Base query structure
        full_query = f"""
            WITH A AS (
                SELECT * FROM `property-database-370200.Dataset_v3.PropertyOwnerDetailsCTE`
                {"WHERE " + null_checks_query if null_checks_query else ""}
            )
        """

        if outside_filters_query:
            full_query += (
                f"\nSELECT COUNT(*) AS CNT FROM A WHERE {outside_filters_query}"
            )
        else:
            full_query += f"\nSELECT COUNT(*) AS CNT FROM A"

        # For distinct owner count query
        updated_query_owner = full_query.replace(
            "COUNT(*) AS CNT", "COUNT(DISTINCT Owner_ID) AS CNT"
        )

        # Log the final queries
        # print(f"Full query: {full_query}")
        # print(f"Distinct owner query: {updated_query_owner}")

        # Execute the queries
        countdf1, countdf2 = (
            bigQueryFetchInstance.runQuery(queryString=full_query),
            bigQueryFetchInstance.runQuery(queryString=updated_query_owner),
        )

        return jsonify(
            {
                "message": "success",
                "count": {
                    "all_rows": str(countdf1["CNT"][0]),
                    "distinct_owners": str(countdf2["CNT"][0]),
                },
            }
        ), 200

    except Exception as e:
        # print("cannot fetch records")
        # print(e)
        traceback.print_exc()
        return render_template(
            "error/page-500.html",
            context={"message": "Could not fetch records", "error": str(e)},
        )


@app.route("/get-location-filters", methods=["GET"])
def get_location_values():
    try:
        location_fields_cached = cache.get(LOCATION_CACHE_KEY)

        if location_fields_cached is None:
            location_fields_database = (
                bigQueryFetchInstance.prop_states_counties_cities()
            )

            if location_fields_database is None or not isinstance(
                location_fields_database, dict
            ):
                return (
                    jsonify(
                        {
                            "message": "Internal server error",
                            "error": "Choices for Prop fields could not be fetched",
                        }
                    ),
                    500,
                )
            else:
                if len(list(location_fields_database.keys())) > 0 and "message" in list(
                    location_fields_database.keys()
                ):
                    if str(
                        location_fields_database["message"]
                    ).strip().lower() == "success" and "data" in list(
                        location_fields_database.keys()
                    ):
                        cache.set(
                            LOCATION_CACHE_KEY,
                            location_fields_database["data"],
                            timeout=600,
                        )

                        return jsonify(location_fields_database), 200

                    elif (
                        str(location_fields_database["message"]).strip().lower()
                        == "result from database is unprocessable"
                    ):
                        return jsonify(location_fields_database), 422
                    else:
                        return jsonify(location_fields_database), 500
                else:
                    return (
                        jsonify(
                            {
                                "message": "Unprocessable entity",
                                "error": "Choices for Prop fields could not be fetched",
                            }
                        ),
                        422,
                    )
        else:
            # print("location_fields_cached is")
            # print(type(location_fields_cached))
            return jsonify({"message": "success", "data": location_fields_cached})
    except Exception as e:
        # print(e)
        traceback.print_exc()
        return jsonify({"message": "Internal server error", "error": str(e)}), 500


# route to get well
@app.route("/get-wells", methods=["GET"])
def get_wells():
    try:
        cached_wells = cache.get(WELLS_CACHE_KEY)

        if cached_wells is None:
            wells = bigQueryFetchInstance.get_wells()

            cache.set(WELLS_CACHE_KEY, wells, timeout=600)

            wells_choices = [(well, well) for well in wells]

            return jsonify(wells_choices)
        else:
            wells_choices = [(well, well) for well in cached_wells]

            return jsonify(wells_choices)
        # pass
    except Exception as e:
        # print("Exception while fetching wells")
        # print(e)
        traceback.print_exc()
        return jsonify([])


# route to get tree coverage
@app.route("/get-tree-coverages", methods=["GET"])
def get_tree_coverage():
    try:
        cached_tree_coverage = cache.get(TREE_COVERAGE_CACHE_KEY)

        if cached_tree_coverage is None:
            tree_coverage = bigQueryFetchInstance.get_tree_coverage()

            cache.set(TREE_COVERAGE_CACHE_KEY, tree_coverage, timeout=600)

            tree_coverage_choices = [(well, well) for well in tree_coverage]

            return tree_coverage_choices
        else:
            tree_coverage_choices = [(well, well) for well in cached_tree_coverage]

            return jsonify(tree_coverage_choices)
    except Exception as e:
        # print("Exception while fetching tree coverage")
        # print(e)
        traceback.print_exc()
        return jsonify([])


# route to get owner type
@app.route("/get-owner-types", methods=["GET"])
def get_owner_type():
    try:
        cached_owner_types = cache.get(OWNER_TYPE_CACHE_KEY)

        if cached_owner_types is None:
            owner_types = bigQueryFetchInstance.get_owner_type()

            # print("owner types")
            # print(owner_types)

            if owner_types is None:
                return jsonify([])

            cache.set(OWNER_TYPE_CACHE_KEY, owner_types, timeout=600)

            owner_type_choices = [
                (owner_type, owner_type) for owner_type in owner_types
            ]

            return jsonify(owner_type_choices)
        else:
            owner_type_choices = [
                (owner_type, owner_type) for owner_type in cached_owner_types
            ]

            return jsonify(owner_type_choices)
    except Exception as e:
        # print("Exception while fetching owner types")
        # print(e)
        traceback.print_exc()
        return jsonify([])


# route to get access type
@app.route("/get-access-types", methods=["GET"])
def get_access_types():
    try:
        cached_access_type = cache.get(ACCESS_TYPE_CACHE_KEY)

        if cached_access_type is None:
            access_types = bigQueryFetchInstance.get_access_type()

            # print(f"access_types {access_types}")

            if access_types is None:
                return jsonify([])
            cache.set(ACCESS_TYPE_CACHE_KEY, access_types, timeout=600)

            access_type_choices = [
                (str(access_type).title(), str(access_type).lower())
                for access_type in access_types
            ]
            return jsonify(access_type_choices)
        else:
            # print(f"Cached access types {cached_access_type}")
            access_type_choices = [
                (str(access_type).title(), str(access_type).lower())
                for access_type in cached_access_type
            ]

            return jsonify(access_type_choices)

    except Exception as e:
        # print("Exception while fetching access types")
        # print(e)
        traceback.print_exc()
        return jsonify([])


@app.route("/get-range", methods=["POST"])
def get_range():
    # print(("get range called"))
    # print(request.json)
    field_name = request.json.get("field", "")

    # print(field_name)

    if (
        field_name is None
        or not isinstance(field_name, str)
        or (isinstance(field_name, str) and len(field_name.strip()) <= 0)
    ):
        return jsonify({"message": "Invalid Request Body parameter value"}), 400
    else:
        field_name = field_name.strip().title()

        if field_name in list(field_dicts.keys()):
            field_name_actual = field_dicts[field_name]

            result = bigQueryFetchInstance.get_min_max_for_field(
                field_name=field_name_actual
            )

            if (
                result is None
                or not isinstance(result, dict)
                or (
                    isinstance(result, dict)
                    and (
                        "min" not in list(result.keys())
                        or "max" not in list(result.keys())
                    )
                )
            ):
                return (
                    jsonify(
                        {
                            "message": f"Could not fetch min max for field {field_name}",
                            "error": f"{result}",
                        }
                    ),
                    500,
                )
            return jsonify(result)

        else:
            return jsonify({"message": "Non Existent field referred"}), 400


@cache.cached(timeout=3600)
def fetch_bigquery_records():
    try:
        results = bigQueryFetchInstance.get_all_records()

        if (
            results is not None
            and isinstance(results, pd.DataFrame)
            and results.shape[0] >= 0
        ):
            return results

        else:
            return []

    except Exception as e:
        # print(f"Exception {e} while fetching bigquery records")
        traceback.print_exc()
        return None


@app.route("/data")
def display_data():
    try:
        page = request.args.get("page", 1, type=int)
        df = fetch_bigquery_records()

        if df is None or not isinstance(df, pd.DataFrame):
            return jsonify({"message": "failed", "error": f"{type(df)}"}), 422

        # Pagination
        per_page = 10  # Number of rows per page
        total_rows = df.shape[0]
        num_pages = (total_rows + per_page - 1) // per_page
        start_idx = (page - 1) * per_page
        end_idx = min(start_idx + per_page, total_rows)

        # Get rows for the current page
        page_data = df.iloc[start_idx:end_idx]

        # page_json = page_data.to_json(orient='records')
        return render_template("result.html", page_data=page_data, num_pages=num_pages)

    except Exception as e:
        # print(e)
        traceback.print_exc()
        return jsonify({"message": "failed", "error": e}), 500


@app.get("/view-records")
def view_records():
    with open("/tmp/query.txt", "r") as fp:
        query = fp.read()

    # sendable_query = query

    # results = pd.read_csv('export.csv')
    page = request.args.get("page", 1, type=int)
    limit = per_page = 10  # Number of rows per page
    offset = (page - 1) * limit

    limit_offset = f"\nLIMIT {limit} OFFSET {offset}"

    query += limit_offset

    # query = fetch_v13_prefix + "\n" + query
    # print(query)

    results = bigQueryFetchInstance.runQuery(queryString=query)

    # return render_template("error/page-500.html", message=results, error=query)

    # Pagination
    # total_rows = results.shape[0]
    # print(total_rows, "<<total_rows")
    # num_pages = (total_rows + per_page - 1) // per_page
    # start_idx = (page - 1) * per_page
    # end_idx = min(start_idx + per_page, total_rows)

    prev = True if page > 1 else False
    next = True if results.shape[0] == per_page else False

    data = results.to_html(classes="table table-striped", index=False)

    return render_template("result.html", data=data, page=page, prev=prev, next=next)


def split_groups_evenly(df, num_groups):
    """Splits DataFrame into evenly sized groups."""
    df_size = len(df)
    avg_size = df_size // num_groups
    remainder = df_size % num_groups
    sizes = [avg_size + 1] * remainder + [avg_size] * (num_groups - remainder)
    return np.split(df, np.cumsum(sizes[:-1]))


def assign_control_numbers(df):
    """Assigns unique random control numbers to unique combinations of columns."""
    min_value = 1
    max_value = 999999

    # Identify unique combinations and their count
    unique_combinations = df[
        ["Owner_ID", "Property_State_Name", "Property_County_Name"]
    ].drop_duplicates()
    num_combinations = len(unique_combinations)

    # Generate unique random control numbers
    if num_combinations > (max_value - min_value + 1):
        raise ValueError(
            "Number of unique combinations exceeds the available range for control numbers."
        )

    control_numbers = random.sample(range(min_value, max_value + 1), num_combinations)

    control_numbers = [
        str(control_number).zfill(7) for control_number in control_numbers
    ]

    # Map unique combinations to control numbers
    unique_combinations["control_number"] = control_numbers
    control_number_map = unique_combinations.set_index(
        ["Owner_ID", "Property_State_Name", "Property_County_Name"]
    )["control_number"].to_dict()

    # Assign control numbers to the DataFrame
    df["control_number"] = df.apply(
        lambda row: control_number_map[
            (row["Owner_ID"], row["Property_State_Name"], row["Property_County_Name"])
        ],
        axis=1,
    )

    return df


@app.route("/export-records", methods=["GET"])
def export_records():
    try:
        # Read the query from file
        with open("/tmp/query.txt", "r") as fp:
            query = fp.read()

        # print("starting download")
        flash("File will be downloading, Please do not close this tab")

        # Fetch results (Replace with actual data fetch logic)
        results = bigQueryFetchInstance.runQuery(queryString=query)
        df = pd.DataFrame(results)

        # Step 1: Sort the DataFrame
        df = df.sort_values(
            by=["Owner_ID", "Property_State_Name", "Property_County_Name", "APN"]
        )

        # Step 2: Pre-process the DataFrame using the PropertyRecordsPreProcessor
        processor = PropertyRecordsPreProcessor(dataframe=df)
        processed_df = processor.pre_process_fetched_results()

        if (
            isinstance(processed_df, dict)
            and "message" in processed_df
            and processed_df["message"] == "failed"
        ):
            # print(f"Error: {processed_df['error']}")
            return jsonify({"error": processed_df["error"]})

        # Step 3: Apply market price filter
        market_price_min = request.args.get("marketPriceMin", type=float, default=None)
        market_price_max = request.args.get("marketPriceMax", type=float, default=None)

        if market_price_min is not None:
            processed_df = processed_df[
                processed_df["Market_Price"] >= market_price_min
            ]

        if market_price_max is not None:
            processed_df = processed_df[
                processed_df["Market_Price"] <= market_price_max
            ]

        # Step 4: Group by 'Owner_ID', 'Property_State_Name', and 'Property_County_Name'
        grouped_df = (
            processed_df.groupby(
                ["Owner_ID", "Property_State_Name", "Property_County_Name"]
            )
            .agg(
                {
                    "APN": lambda x: x.tolist(),
                    "Lot_Acreage": lambda x: x.tolist(),
                    "Market_Price": lambda x: x.tolist(),
                    "Offer_Price": lambda x: x.tolist(),
                    "Property_Zip_Code": lambda x: x.tolist(),
                    # "Final_Offer_Price": lambda x: x.tolist(),
                    "Owner_Full_Name": "first",
                    "Owner_Last_Name": "first",
                    "Owner_First_Name": "first",
                    "Owner_Short_Name": "first",
                    "Owner_Name_Type": "first",
                    "Mail_City": "first",
                    "Mail_State": "first",
                    "Mail_Street_Address": "first",
                    "Mail_Zip_Code": "first",
                }
            )
            .reset_index()
        )

        def calculate_final_offer_price(row):
            total_offer_price = sum(
                row["Offer_Price"]
            )  # Sum of all Offer_Price values in the group
            random_amount = np.random.uniform(
                1.01, 99.99
            )  # Random amount between 1.01 and 99.99
            return round(total_offer_price + random_amount, 2)  # Final Offer Price

        # Apply the function to calculate Final_Offer_Price for each group
        grouped_df["Final_Offer_Price"] = grouped_df.apply(
            calculate_final_offer_price, axis=1
        )

        # Step 5: Assign control numbers
        grouped_df = assign_control_numbers(grouped_df)

        # Step 6: Shuffle the combinations
        shuffled_df = grouped_df.sample(frac=1).reset_index(drop=True)

        # Create reference number
        mail_groups = request.args.get("mailGroups", "").split(",")
        num_mail_groups = len(mail_groups)

        if num_mail_groups == 0:
            return jsonify({"error": "No mail groups provided"})

        # Split the shuffled DataFrame into evenly sized groups
        split_groups = split_groups_evenly(shuffled_df, num_mail_groups)

        # Create a zip buffer to hold the CSVs
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            for i, (group, mail_group_name) in enumerate(
                zip(split_groups, mail_groups)
            ):
                # Add mail group name
                group["Mailer_Group"] = mail_group_name

                # Update the reference number to include both mail group and control number
                group["reference_number"] = group.apply(
                    lambda row: f"{mail_group_name}-{row['control_number']}", axis=1
                )

                # Convert group DataFrame to CSV
                csv_buffer = io.BytesIO()
                group.to_csv(csv_buffer, index=False)
                csv_buffer.seek(0)

                # Add CSV to zip
                zipf.writestr(f"{mail_group_name}.csv", csv_buffer.getvalue())

        buffer.seek(0)

        return send_file(
            buffer,
            as_attachment=True,
            download_name="records.zip",
            mimetype="application/zip",
        )

    except Exception as e:
        # print(e)
        return jsonify({"error": str(e)})


@cache.cached(timeout=3600)
@app.route("/get-records", methods=["POST"])
def fetch_records():
    try:
        request_body = request.get_json(silent=True)
        if request_body is None:
            return jsonify({"message": "No JSON data received"}), 400

        filters = {}
        null_checks = []
        outside_filters = []

        # Separate filters and null checks
        for key, value in request_body.items():
            # if key.endswith("-include-null"):
            #     field_name = key.replace("-include-null", "")
            #     if value:  # include-null is true
            #         if key == "zip-code-matching-include-null":
            #             null_checks.append(
            #                 "Property_Zip_Code IS NULL OR Mail_Zip_Code IS NULL OR "
            #                 "Mail_Zip_Code_9 IS NULL OR Mail_Zip_Code_R IS NULL OR "
            #                 "Mail_ZipCode_STD IS NULL"
            #             )
            #         else:
            #             null_checks.append(
            #                 f"{field_schema_mapping.get(field_name)} IS NULL"
            #             )
            # else:
            filters[key] = value

        # Helper function to handle range filters
        def range_filter(field_name, field_value):
            return {
                "min": number_input_validator(
                    value=field_value.get("min"), field_name=f"{field_name}-min"
                ),
                "max": number_input_validator(
                    value=field_value.get("max"), field_name=f"{field_name}-max"
                ),
            }

        # Construct the subqueries
        for key, value in filters.items():
            sub_query = None
            include_null = False  # Track if we should include NULL condition

            if key == "zip-code-matching" and value is not None:
                sub_query = customQueryBuilderInstance.build_query(
                    field_name=key,
                    filter_values=value,
                )

                # if key == "owner-do-not-mail" and value is not None:
                print(key, "value--------", value)
            #     sub_query = customQueryBuilderInstance.build_query(
            #         field_name=key,
            #         filter_values=value,
            #     )
            if key in field_schema_mapping:
                mapped_field_name = field_schema_mapping[key]

                # Check if the field has an include-null flag
                include_null_key = f"{key}-include-null"
                include_null = (
                    include_null_key in request_body and request_body[include_null_key]
                )

                if isinstance(value, dict) and ("min" in value or "max" in value):
                    filter_values = range_filter(key, value)
                    sub_query = customQueryBuilderInstance.build_query(
                        field_name=mapped_field_name,
                        filter_values=filter_values,
                    )
                elif isinstance(value, list) and value:
                    sub_query = customQueryBuilderInstance.build_query(
                        field_name=mapped_field_name,
                        filter_values=value,
                    )
                elif isinstance(value, (int, float)):
                    sub_query = customQueryBuilderInstance.build_query(
                        field_name=mapped_field_name,
                        filter_values=value,
                    )
                elif isinstance(value, str) and value.strip():
                    sub_query = customQueryBuilderInstance.build_query(
                        field_name=mapped_field_name,
                        filter_values=value,
                    )

            # Add to appropriate filter list
            if sub_query:
                # If include-null is true, add the sub_query and IS NULL combined inside WITH A
                if include_null:
                    null_checks.append(f"({sub_query} OR {mapped_field_name} IS NULL)")
                else:
                    outside_filters.append(sub_query)

        # Build the final query with WITH A AS block and outside WHERE clause
        null_checks_query = " AND ".join(null_checks)
        outside_filters_query = " AND ".join(outside_filters)

        # Base query structure
        full_query = f"""
            WITH A AS (
                SELECT * FROM `property-database-370200.Dataset_v3.PropertyOwnerDetailsCTE`
                {"WHERE " + null_checks_query if null_checks_query else ""}
            )
        """

        if outside_filters_query:
            full_query += f"\nSELECT * FROM A WHERE {outside_filters_query}"
        else:
            full_query += "\nSELECT * FROM A"

        # print("updated query")
        # print(">>\n", full_query, "\n<<")

        # Save the query to a file (optional)
        with open("/tmp/query.txt", "w") as fb:
            fb.write(full_query)

        return redirect("/view-records")

    except Exception as e:
        # print("cannot fetch records")
        # print(e)
        traceback.print_exc()
        return jsonify({"message": "Failed to fetch records"}), 500


# def call_cloud_function_for_data_processing(query):
#     # print("sending request")

#     response = requests.post(
#         Export_Processor_URL,
#         params={"api_key": CLOUD_API_GATEWAY_KEY},
#         json={
#             "query": query,
#         },
#     )

# print(response)
# print(type(response))


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route("/upload", methods=["GET", "POST"])
def upload():
    if request.method == "POST":
        if "file" not in request.files:
            flash("No file part")
            return redirect(request.url)

        file = request.files["file"]

        if file.filename == "":
            flash("No selected file")
            return redirect(request.url)

        if file and allowed_file(file.filename):
            if file.content_length > MAX_CONTENT_LENGTH:
                flash("File size exceeds the limit (1 GB)")
                return redirect(request.url)

            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config["UPLOAD_FOLDER"], filename))
            flash("File successfully uploaded")

            # upload file to cloud storage.

            # get absolute file path
            file_path_absolute = pathlib.Path(
                f"{app.config['UPLOAD_FOLDER']}/{filename}"
            ).resolve()
            # print(file_path_absolute)

            file_path_absolute_string = str(file_path_absolute)
            # print(file_path_absolute_string)

            upload_result = CLOUD_STORAGE_CLIENT.upload_file_to_cloud_bucket(
                bucket_name=REMOTE_BUCKET_NAME_FOR_UPLOAD_CSV_FROM_MAIL_HOUSE,
                destination_directory=REMOTE_DIRECTORY_FOR_UPLOAD_CSV_FROM_MAIL_HOUSE,
                file_name=filename,
                file_path_absolute=file_path_absolute_string,
                uploadable_file_name=filename,
            )

            if (
                upload_result is not None
                and isinstance(upload_result, str)
                and len(upload_result.strip()) > 0
            ):
                # print("cloud storage upload result success")
                # print("create orphaned thread to make cloud function api call")
                t1 = threading.Thread(
                    target=call_to_generate_pdf_cloud_function,
                    args={
                        filename,
                    },
                )
                t1.start()

            else:
                print("cloud storage upload result failed")

            return redirect(url_for("acknowledgment"))
        else:
            flash("Invalid file type. Only CSV files are allowed.")
            return redirect(request.url)
    return render_template("upload.html")


def call_to_generate_pdf_cloud_function(file_name):
    try:
        response = requests.post(
            PDF_GENERATOR_URL,
            params={"api_key": CLOUD_API_GATEWAY_KEY},
            json={
                "bucket-name": REMOTE_BUCKET_NAME_FOR_UPLOAD_CSV_FROM_MAIL_HOUSE,
                "saved-directory": REMOTE_DIRECTORY_FOR_UPLOAD_CSV_FROM_MAIL_HOUSE,
                "file-name": file_name,
            },
        )

        # print(response)
        # print(type(response))
    except Exception as e:
        traceback.print_exc()
        # print(f"Exception {e} while calling generate pdf cloud function")


def call_to_generate_pdf_cloud_function_v2(file_path):
    try:
        response = requests.post(
            PDF_GENERATOR_URL,
            params={"api_key": CLOUD_API_GATEWAY_KEY},
            json={"file-path": file_path},
        )

        # print(response)
        # print(type(response))
    except Exception as e:
        traceback.print_exc()
        # print(f"Exception {e} while calling generate pdf cloud function")


@app.route("/acknowledgment")
def acknowledgment():
    return render_template("acknowledgment.html")


@app.get("/dummy")
def test_dropdown_ui():
    return render_template("dummy.html")



uploaded_df = None  # Variable to store the uploaded DataFrame
merged_df = None  # Variable to store the full merged DataFrame

@app.route("/upload_csv", methods=["POST"])
def upload_csv():
    global uploaded_df
    file = request.files["file"]

    if file:
        uploaded_df = pd.read_csv(file)
        columns = list(uploaded_df.columns)
        return jsonify({"columns": columns})

    return jsonify({"error": "No file uploaded"}), 400


@app.route("/merge_columns", methods=["POST"])
def merge_columns():
    global uploaded_df, merged_df
    try:
        data = request.json  # Get the merge data from the frontend

        if uploaded_df is not None:
            # Create a local copy of the uploaded DataFrame
            df_copy = uploaded_df.copy()

            for main_column, columns_to_merge in data.items():
                # Check if columns_to_merge exist in the DataFrame
                if not all(col in df_copy.columns for col in columns_to_merge):
                    return jsonify(
                        {
                            "error": f"One or more columns to merge are missing in the DataFrame"
                        }
                    ), 400

                # Merge selected columns into the main column
                df_copy[main_column] = df_copy[columns_to_merge].apply(
                    lambda row: " ".join(row.dropna().astype(str)), axis=1
                )

                # Avoid dropping the main column itself, in case it's part of the merge list
                columns_to_drop = [col for col in columns_to_merge if col != main_column]
                df_copy.drop(columns=columns_to_drop, inplace=True)

            # Store the full merged DataFrame globally
            merged_df = df_copy.copy()

            # Handle NaN values before sending response for preview (first 10 rows)
            headers = list(df_copy.columns)
            rows = (
                df_copy.head(10).fillna("").values.tolist()
            )  # Replace NaN with empty string

            # Ensure that rows are in a format compatible with JSON
            rows = [[str(cell) for cell in row] for row in rows]

            return jsonify({"headers": headers, "rows": rows})

        return jsonify({"error": "No data to merge"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route("/upload_merged_csv", methods=["POST"])
def upload_merged_csv():
    global merged_df
    try:
        if merged_df is None:
            return jsonify({"error": "No merged data available for upload"}), 400

        # Save full merged DataFrame to a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            temp_file_path = temp_file.name
            merged_df.to_csv(temp_file_path, index=False)  # Save the full CSV

        # Get current date and format it like "Sep-17-2024"
        current_date = datetime.now().strftime("%b-%d-%Y")
        # Update the directory name to "Campaign-Sep-17-2024"
        campaign_directory = f"Campaign-{current_date}"

        # Upload the full CSV file to cloud storage
        file_path_absolute = pathlib.Path(temp_file_path).resolve()
        file_path_absolute_string = str(file_path_absolute)

        upload_result = CLOUD_STORAGE_CLIENT.upload_file_to_cloud_bucket(
            bucket_name=REMOTE_BUCKET_NAME_FOR_UPLOAD_CSV_FROM_MAIL_HOUSE,
            destination_directory=campaign_directory,  # Use the dynamic directory name
            file_name=os.path.basename(temp_file_path),
            file_path_absolute=file_path_absolute_string,
            uploadable_file_name=os.path.basename(temp_file_path),
        )

        print(upload_result)

        os.remove(temp_file_path)

        if upload_result:
            # Create the file path in the cloud storage
            cloud_file_path = f"{REMOTE_BUCKET_NAME_FOR_UPLOAD_CSV_FROM_MAIL_HOUSE}/{campaign_directory}/{os.path.basename(temp_file_path)}"

            # Start a thread to call the cloud function
            print("Cloud storage upload result success")
            print("Creating orphaned thread to make cloud function API call")
            t1 = threading.Thread(
                target=call_to_generate_pdf_cloud_function_v2,
                args=(cloud_file_path,)
            )
            t1.start()

            return jsonify({"message": f"File successfully uploaded to '{campaign_directory}' in cloud storage, and PDF generation is in progress. You will be notified via email once it's completed."}), 200
        else:
            return jsonify({"error": "Failed to upload file to cloud storage"}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500




if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
