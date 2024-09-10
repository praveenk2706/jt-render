import io
import os
import pathlib
import random
import sys
import threading
import traceback
import zipfile

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

print(sys.getrecursionlimit())
sys.setrecursionlimit(10000)
print(sys.getrecursionlimit())

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
    return render_template("index.html")


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
        print(f"Exception {e} while getting filter values")
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
        print(f"exception {e} in validating {value} for field {field_name}")
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

        for key, value in request_body.items():
            if key.endswith("-include-null"):
                field_name = key.replace("-include-null", "")
                if value:
                    if key == "zip-code-matching-include-null":
                        null_checks.append(
                            "Property_Zip_Code IS NULL OR Mail_Zip_Code IS NULL OR "
                            "Mail_Zip_Code_9 IS NULL OR Mail_Zip_Code_R IS NULL OR "
                            "Mail_ZipCode_STD IS NULL"
                        )
                    else:
                        # Add NULL check for other fields
                        null_checks.append(
                            f"{field_schema_mapping.get(field_name)} IS NULL"
                        )
            else:
                filters[key] = value

        print("Filters\n", filters)
        # base_query = fetch_v13_prefix
        sub_queries = []

        def range_filter(field_name, field_value):
            return {
                "min": number_input_validator(
                    value=field_value.get("min"), field_name=f"{field_name}-min"
                ),
                "max": number_input_validator(
                    value=field_value.get("max"), field_name=f"{field_name}-max"
                ),
            }

        for key, value in filters.items():
            if key == "zip-code-matching" and value is not None:
                sub_query = customQueryBuilderInstance.build_query(
                    field_name=key,
                    filter_values=value,
                )
                print(sub_query, "zipppppppppppppppppppppppppppppppp")
            if key in field_schema_mapping:
                mapped_field_name = field_schema_mapping[key]
                print(mapped_field_name, "mapped_field_name--------")
                print(value, "dict")

                if isinstance(value, dict) and ("min" in value or "max" in value):
                    print(value, "dict")
                    # Range filter case
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

            if sub_query:
                sub_queries.append(sub_query)

        # Combine subqueries and null checks
        query_end = ""
        if sub_queries:
            # Start with the sub_queries combined by AND
            query_end = " WHERE " + " AND ".join(sub_queries)

            if null_checks:
                # Combine null_checks with sub_queries using OR
                query_end += " OR (" + " OR ".join(null_checks) + ")"

        print(f"Query End {query_end}")

        # sub_query = " AND ".join(sub_queries)
        print("sub queries")
        print(sub_queries)
        sub_queries = [
            str(sub_query).upper().replace("WHERE", "") for sub_query in sub_queries
        ]
        updated_query_all = (
            "SELECT COUNT(*) AS CNT FROM `property-database-370200.Dataset_v3.PropertyOwnerDetailsCTE` "
            + query_end
        )
        updated_query_owner = (
            "SELECT COUNT(DISTINCT Owner_ID) AS CNT FROM `property-database-370200.Dataset_v3.PropertyOwnerDetailsCTE` "
            + query_end
        )

        print("updated query")

        countdf1, countdf2 = (
            bigQueryFetchInstance.runQuery(queryString=updated_query_all),
            bigQueryFetchInstance.runQuery(queryString=updated_query_owner),
        )

        print(countdf2)
        print(countdf1)

        return jsonify(
            {
                "message": "success",
                "count": {
                    "all_rows": str(countdf1["CNT"][0]),
                    "distinct_owners": str(countdf2["CNT"][0]),
                },
            }
        ), 200

        # print(">>\n", updated_query, "\n<<")

        # results.to_csv('export.csv')
        # with open("/tmp/query.txt", "w") as fb:
        #     # fb.write(updated_query)
        # fb.close()

        return redirect("/view-records")

    except Exception as e:
        print("cannot fetch records")
        print(e)
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
            print("location_fields_cached is")
            print(type(location_fields_cached))
            return jsonify({"message": "success", "data": location_fields_cached})
    except Exception as e:
        print(e)
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
        print("Exception while fetching wells")
        print(e)
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
        print("Exception while fetching tree coverage")
        print(e)
        traceback.print_exc()
        return jsonify([])


# route to get owner type
@app.route("/get-owner-types", methods=["GET"])
def get_owner_type():
    try:
        cached_owner_types = cache.get(OWNER_TYPE_CACHE_KEY)

        if cached_owner_types is None:
            owner_types = bigQueryFetchInstance.get_owner_type()

            print("owner types")
            print(owner_types)

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
        print("Exception while fetching owner types")
        print(e)
        traceback.print_exc()
        return jsonify([])


# route to get access type
@app.route("/get-access-types", methods=["GET"])
def get_access_types():
    try:
        cached_access_type = cache.get(ACCESS_TYPE_CACHE_KEY)

        if cached_access_type is None:
            access_types = bigQueryFetchInstance.get_access_type()

            print(f"access_types {access_types}")

            if access_types is None:
                return jsonify([])
            cache.set(ACCESS_TYPE_CACHE_KEY, access_types, timeout=600)

            access_type_choices = [
                (str(access_type).title(), str(access_type).lower())
                for access_type in access_types
            ]
            return jsonify(access_type_choices)
        else:
            print(f"Cached access types {cached_access_type}")
            access_type_choices = [
                (str(access_type).title(), str(access_type).lower())
                for access_type in cached_access_type
            ]

            return jsonify(access_type_choices)

    except Exception as e:
        print("Exception while fetching access types")
        print(e)
        traceback.print_exc()
        return jsonify([])


@app.route("/get-range", methods=["POST"])
def get_range():
    print(("get range called"))
    print(request.json)
    field_name = request.json.get("field", "")

    print(field_name)

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
        print(f"Exception {e} while fetching bigquery records")
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
        print(e)
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
    print(query)

    results = bigQueryFetchInstance.runQuery(queryString=query)

    # return render_template("error/page-500.html", message=results, error=query)

    # Pagination
    # total_rows = results.shape[0]
    # print(total_rows,'<<total_rows')
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
        str(control_number).zfill(6) for control_number in control_numbers
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

        print("starting download")
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

        print("process df ", list(processed_df.columns))

        if (
            isinstance(processed_df, dict)
            and "message" in processed_df
            and processed_df["message"] == "failed"
        ):
            print(f"Error: {processed_df['error']}")
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

        # print("process df ", list(processed_df.columns))

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
                    "Final_Offer_Price": lambda x: x.tolist(),
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
        # grouped_df = processed_df.groupby(
        #     ["Owner_ID", "Property_State_Name", "Property_County_Name"]
        # ).apply(lambda x: x.reset_index(drop=True))

        # print(grouped_df.columns)

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
        print(e)
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

        print(request_body)

        for key, value in request_body.items():
            if key.endswith("-include-null"):
                field_name = key.replace("-include-null", "")
                if value:
                    if key == "zip-code-matching-include-null":
                        null_checks.append(
                            "Property_Zip_Code IS NULL OR Mail_Zip_Code IS NULL OR "
                            "Mail_Zip_Code_9 IS NULL OR Mail_Zip_Code_R IS NULL OR "
                            "Mail_ZipCode_STD IS NULL"
                        )
                    else:
                        # Add NULL check for other fields
                        null_checks.append(
                            f"{field_schema_mapping.get(field_name)} IS NULL"
                        )
            else:
                filters[key] = value

        # Process filters and range filters as needed
        sub_queries = []

        def range_filter(field_name, field_value):
            return {
                "min": number_input_validator(
                    value=field_value.get("min"), field_name=f"{field_name}-min"
                ),
                "max": number_input_validator(
                    value=field_value.get("max"), field_name=f"{field_name}-max"
                ),
            }

        for key, value in filters.items():
            if key == "zip-code-matching" and value is not None:
                sub_query = customQueryBuilderInstance.build_query(
                    field_name=key,
                    filter_values=value,
                )
            if key in field_schema_mapping:
                mapped_field_name = field_schema_mapping[key]
                print(mapped_field_name, "mapped_field_name--------")
                print(value, "dict")

                if isinstance(value, dict) and ("min" in value or "max" in value):
                    print(value, "dict")
                    # Range filter case
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

                if sub_query:
                    sub_queries.append(sub_query)

        # Combine subqueries and null checks
        query_end = ""
        if sub_queries:
            # Start with the sub_queries combined by AND
            query_end = " WHERE " + " AND ".join(sub_queries)

            if null_checks:
                # Combine null_checks with sub_queries using OR
                query_end += " OR (" + " OR ".join(null_checks) + ")"

        updated_query = (
            "SELECT * FROM `property-database-370200.Dataset_v3.PropertyOwnerDetailsCTE` "
            + query_end
        )

        print("updated query")
        print(">>\n", updated_query, "\n<<")

        # Save the query to a file (optional)
        with open("/tmp/query.txt", "w") as fb:
            fb.write(updated_query)

        return redirect("/view-records")

    except Exception as e:
        print("cannot fetch records")
        print(e)
        traceback.print_exc()
        return jsonify({"message": "Failed to fetch records"}), 500


@app.route("/export-data", methods=["GET"])
def export_data():
    query = ""
    with open("/tmp/query.txt", "r") as fp:
        query = fp.read()

    print(query)

    if query.strip() == "":
        return jsonify({"message": "failed"}), 500

    t1 = threading.Thread(target=call_cloud_function_for_data_processing, args=(query,))
    t1.start()

    return {
        "message": "success",
        "data": "Exported Data Procecssing started. You would be notified over email.",
    }

    # return response


def call_cloud_function_for_data_processing(query):
    print("sending request")

    response = requests.post(
        Export_Processor_URL,
        params={"api_key": CLOUD_API_GATEWAY_KEY},
        json={
            "query": query,
        },
    )

    print(response)
    print(type(response))


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
            print(file_path_absolute)

            file_path_absolute_string = str(file_path_absolute)
            print(file_path_absolute_string)

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
                print("cloud storage upload result success")
                print("create orphaned thread to make cloud function api call")
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

        print(response)
        print(type(response))
    except Exception as e:
        traceback.print_exc()
        print(f"Exception {e} while calling generate pdf cloud function")


@app.route("/acknowledgment")
def acknowledgment():
    return render_template("acknowledgment.html")


@app.get("/dummy")
def test_dropdown_ui():
    return render_template("dummy.html")


if __name__ == "__main__":
    app.run(debug=True, port=8081)
