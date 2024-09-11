import json
import traceback

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from .fetch_query_base import fetch_v12_prefix, fetch_v13_prefix  # noqa: F401


class BigQueryFetcher:
    def __init__(self) -> None:
        self._project_id = "property-database-370200"
        self._service_account_email = (
            "my-bigquery-sa@property-database-370200.iam.gserviceaccount.com"
        )

        self._dataset_name = "Dev_Dataset_2"
        self.BIGQUERY_TABLE = "property-database-370200.Dev_Dataset_2.Properties"
        # data = None
        # with open("./service_accounts.json") as f:
        #     data = json.load(f)

        # if data is not None and isinstance(data, list) and len(data) > 0:
        #     for item in data:
        #         if (
        #             item is not None
        #             and isinstance(item, dict)
        #             and "associated" in list(item.keys())
        #             and "info" in list(item.keys())
        #         ):
        #             if (
        #                 str(item["associated"]).lower() == "bigquery"
        #                 and item["info"] is not None
        #             ):
        #                 self.service_account_info = item["info"]
        #                 break
        self.service_account_info = {
            "type": "service_account",
            "project_id": "property-database-370200",
            "private_key_id": "c6e2d46502526baa73edd6ea144395bd3c964847",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDCiKfCp50j6WM6\nLuWmhwgmCgCghYrISzJaMLrLIG/7Bki9UQdR6ARKORrYUuTuSFkPLYr3ivZ6pK9/\n+kz+I9AC7m6GsfJMEkAN9WWspwkUrk5Eort7+dYQQOiwB1q7e/c6inNUrhANExWA\nkFKMNa25pgqR83QtiRAfT2HSo5IXojUsX9whf3NtE7AoqBNe6vJ77KIudsmcFiFn\nzuW/ekSN3/RIW5mecCenQ/3lpF4e6dxkvx64xvEHlPmgTRguuk2cZJdV3Pmc/E0P\nqr2k1GFUOHH2WVTArMtr3kf/5puBqMFlPfeptU4kvYFa9wi/2mqQfGE1gStUZK9/\nJIUyDJ0/AgMBAAECggEAGWolEKc5PmHVVtqdoh3uEJop7s7DjNtWHrZTEQqR1qfB\nhQo6/JZlSRUgzrY1tnO5PuRiTWjYPrmZPA0FVgZzvI5N3kcNMqcHu0seF2pTI2KK\nUyk3eUDwmHsCa0zqq6fMatd/rcZ3zl1556vzPOIeaiypINw/h1tqwcxrKUu5YGDT\n70SWqZq4zs0igBqwMxgdL6eh/o32hCXXhOUnKzHF5CLKGO/gi1Rmy5B2d2aEL/5J\nKe89+ssbC8XeuRbuuVlvs67MDvbaqOtrwuYr9Y7FGjfuJyo1FkqjJQdBK00IJMfQ\nQpn7d/a0PY3ESU2T2YqZuUd2oxhG/t7LaQ41/s6RUQKBgQD9KtxU60uLG25EfB8m\nbQ+QwRK76Do0zsOMTcngQBWhO8inKQnkZ5p7JvZuUJzdToOCtrrJZSyMpSml1+u2\nFKNAJLx+YPIM+GOX74S8S/CCKpFogklXcz1lbLhN+abo11Bke0Xy8A510KICvm/k\n+xmYza41aGfC3vecTQCul7z4tQKBgQDEtdot3bfufhQhL+DC6Zhl+4eBvxeImYae\nG/kPS1b4qB8gwnOwdDhWsMMvT0YvWjj87LvrTTtgSrYfAt/N2fsJAkGMvCQahADq\nwX0MDzIhCCJU4xlAM+i/aAYoWiQWWywj6bjtpPTB1hxG2L/qFrltH2YQN5L6GFM7\nvn/1EeR6owKBgCjfw6T+PDsgWaQ6+fiFQS8YzQKDkuo1u8KqSH6pOhTHBzVd3df/\nliRt/PmAkGL2qyruf6fGavPmMpwxCUBjgpv3kmtBEbNgYwwDwsV36UPKxaE+78Jr\nu6zlabhFJ1qOcM9YS8nWUi9ZWN7iikRI+i/+fTLoQWBoCUg18nc9zmfFAoGBAKGu\nw00Nv3/Em3EHtFsgu4Wfb6qiUYqSfgZ5AYajqzGopiU8DvELJb+DfHrnUs3a/5pM\nTs1B5rrnzbfBmgjN0Tt/Yjd18MrmkwZh0RFXcCGcWPOi0f9mazjgHKeQJ7dEawx9\nr+WMvyyPeyB0SZHz6acatr4/n4v28HrbwrrBYSW3AoGAOp65UJI2+cFu13fIDlLd\n3oJK8kIeI+uXTZ/CfpEy0g4ECoPuzgXIb43lbin4exkfTfU5cuOdE68Lbw9AgQvU\nWoHRYoGbhlkY0PGX4cEhNEU2PRHQfyHD1lhr6KqLBmFlnMFCY+GrZxbH6APl5T1V\nCyYBKnBC3zfDHHT8aXLwQbc=\n-----END PRIVATE KEY-----\n",
            "client_email": "my-bigquery-sa@property-database-370200.iam.gserviceaccount.com",
            "client_id": "114110166144849774399",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/my-bigquery-sa%40property-database-370200.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com",
            "scopes": [
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery",
            ],
        }

        self._create_connection_instance()

    def _create_connection_instance(self):
        try:
            if hasattr(self, "service_account_info"):
                self._credentials = (
                    service_account.Credentials.from_service_account_info(
                        self.service_account_info
                    )
                )

                try:
                    self._client = bigquery.Client(
                        credentials=self._credentials, project=self._project_id
                    )
                except Exception as e2:
                    print("cannot create bigquery connection")
                    print(e2)
                    self._client = None
            else:
                self._credentials = None
                self._client = None
        except Exception as e:
            print(f"could not create service account credentials {e}")

            self._credentials = None
            self._client = None

    @staticmethod
    def is_valid_string(value):
        if (
            value is None
            or not isinstance(value, str)
            or (isinstance(value, str) and not len(value.strip()) > 0)
        ):
            return False
        else:
            return True

    @staticmethod
    def return_valid_string(value):
        if (
            value is None
            or not isinstance(value, str)
            or (isinstance(value, str) and not len(value.strip()) > 0)
        ):
            return None
        else:
            return value.strip()

    def runQuery(self, queryString):
        try:
            if self._client is not None and self._credentials is not None:
                # print(f"query: -- {queryString}")
                query_job = self._client.query(query=queryString)

                print(f"query job {type(query_job)}")

                data_frame = query_job.to_dataframe()

                print(f"data frame {type(data_frame)}")

                return data_frame
        except Exception as e:
            print(f"Query {queryString} cannot be run {e}")
            traceback.print_exc()
            return None

    def get_min_max_for_field(self, field_name):
        try:
            query_min = (
                f"{fetch_v12_prefix}SELECT MIN({field_name}) AS MINIMUM FROM Joined"
            )

            min_result = 0.00

            max_result = 999999.00

            min_result = self.runQuery(queryString=query_min)
            print(f"min result {type(max_result)}")
            print(min_result)

            if (
                min_result is not None
                and isinstance(min_result, pd.DataFrame)
                and min_result.shape[0] > 0
            ):
                min_result = min_result["MINIMUM"][0]

            query_max = (
                f"{fetch_v12_prefix}SELECT MAx({field_name}) AS MAXIMUM FROM Joined"
            )
            max_result = self.runQuery(queryString=query_max)

            print(f"max result {max_result} {type(max_result)}")

            if (
                max_result is not None
                and isinstance(max_result, pd.DataFrame)
                and max_result.shape[0] > 0
            ):
                max_result = max_result["MAXIMUM"][0]

            print(f"{field_name} : --- min {min_result} max {max_result}")

            return {"min": min_result, "max": max_result}
        except Exception as e:
            traceback.print_exc()
            print(e)
            return None

    def prop_states_counties_cities(self):
        try:
            # SQL query to retrieve distinct values of State, District, and City
            query = (
                "SELECT DISTINCT Lower(Property_State_Name) AS Property_State_Name, Lower(Property_County_Name) AS Property_County_Name, Lower(Property_City) AS Property_City, Lower(nearest_road_type) AS Nearest_Road_Type "
                + "FROM `property-database-370200.Dataset_v3.PropertyOwnerDetailsCTE` "
                + "GROUP BY Property_State_Name, Property_County_Name, Property_City, Nearest_Road_Type "
                + "ORDER BY Property_State_Name, Property_County_Name, Property_City, Nearest_Road_Type"
            )

            # Execute the query  into a Pandas DataFrame
            result_df = self.runQuery(queryString=query)

            if (
                result_df is None
                or not isinstance(result_df, pd.DataFrame)
                or (isinstance(result_df, pd.DataFrame) and result_df.shape[0] <= 0)
            ):
                return {
                    "message": "Result from database is unprocessable",
                    "error": str(type(result_df)),
                }

            # Initialize a dictionary to store the distinct values
            distinct_values = {}

            # Iterate over the DataFrame to populate the dictionary
            for _, row in result_df.iterrows():
                state = str(row["Property_State_Name"]).title()
                district = str(row["Property_County_Name"]).title()
                city = str(row["Property_City"]).title()
                nearest_road_type = str(row["Nearest_Road_Type"]).title()

                if (
                    state not in distinct_values
                    and str(state).lower() != "no information"
                ):
                    distinct_values[state] = {}

                if (
                    distinct_values[state] is not None
                    and isinstance(distinct_values[state], dict)
                    and district not in distinct_values[state].keys()
                    and str(district).lower() != "no information"
                ):
                    distinct_values[state][district] = []

                if (
                    distinct_values[state] is not None
                    and isinstance(distinct_values[state], dict)
                    and district in distinct_values[state].keys()
                    and distinct_values[state][district] is not None
                    and isinstance(distinct_values[state][district], list)
                    and city not in distinct_values[state][district]
                    and str(city).lower() != "no information"
                ):
                    distinct_values[state][district].append(city)

                if "nearest_road_type" not in distinct_values:
                    distinct_values["nearest_road_type"] = []
                if (
                    nearest_road_type not in distinct_values["nearest_road_type"]
                    and str(nearest_road_type).lower() != "no information"
                ):
                    distinct_values["nearest_road_type"].append(
                        str(nearest_road_type).title()
                    )

            distinct_values["nearest_road_type"].insert(0, "No Information")

            return {"message": "Success", "data": distinct_values}

        except Exception as e:
            print(e)
            traceback.print_exc()
            return {"message": "Internal server error", "error": str(e)}

    def get_nearest_road_type(self):
        try:
            # SQL query to retrieve distinct values of Nearest Road Type
            query = (
                f"{fetch_v12_prefix}"
                + "SELECT DISTINCT Lower(nearest_road_type) AS Nearest_Road_Type "
                + "FROM Joined "
                + "ORDER BY Nearest_Road_Type"
            )

            # Execute the query into a Pandas DataFrame
            result_df = self.runQuery(queryString=query)

            if (
                result_df is None
                or not isinstance(result_df, pd.DataFrame)
                or (isinstance(result_df, pd.DataFrame) and result_df.shape[0] <= 0)
            ):
                return {
                    "message": "Result from database is unprocessable",
                    "error": str(type(result_df)),
                }

            # Initialize a list to store the distinct values
            distinct_values = []

            # Iterate over the DataFrame to populate the list
            for _, row in result_df.iterrows():
                road_type = row["Nearest_Road_Type"].title()
                distinct_values.append(road_type)

            return {"message": "Success", "data": distinct_values}

        except Exception as e:
            print(e)
            traceback.print_exc()
            return {"message": "Internal server error", "error": str(e)}

    def get_all_records(self):
        try:
            query = fetch_v12_prefix + " SELECT * FROM Joined LIMIT 100"

            results = self.runQuery(queryString=query)

            if (
                results is not None
                and isinstance(results, pd.DataFrame)
                and results.shape[0] >= 0
            ):
                return results

            else:
                return {
                    "message": "Error in retrieved properties data",
                    "error": f"{type(results)}",
                }
        except Exception as e:
            traceback.print_exc()
            print(e)
            print("cannot fetch properties from BigQuery")
            return {
                "message": "Error in retrieved properties data",
                "error": f"{str(e)}",
            }

    def get_filtered_rows_count(self):
        pass

    def get_table_schema(self):
        try:
            table = self._client.get_table(self.BIGQUERY_TABLE)

            schema = table.schema

            return schema

        except Exception as e:
            print(f"Exception {e} while returning table schema")
            traceback.print_exc()
            return None

    def numeric_fields_filters(self):
        try:
            schema = self.get_table_schema()

            numeric_fields = [
                field.name
                for field in schema
                if field.field_type in ("INTEGER", "FLOAT", "NUMERIC", "BIGNUMERIC")
            ]

            skip_fields = [
                "acreage-max",
                "acreage-min",
                "estuarine_and_marine_deepwater-max",
                "estuarine_and_marine_deepwater-min",
                "estuarine_and_marine_wetland-max",
                "estuarine_and_marine_wetland-min",
                "freshwater_emergent_wetland-max",
                "freshwater_emergent_wetland-min",
                "freshwater_forested_shrub_wetland-max",
                "freshwater_forested_shrub_wetland-min",
                "freshwater_pond-max",
                "freshwater_pond-min",
                "lake-max",
                "lake-min",
                "lms_mortgage_amount-max",
                "lms_mortgage_amount-min",
                "lms_sale_price-max",
                "lms_sale_price-min",
                "property_latitude-max",
                "property_latitude-min",
                "property_longitude-max",
                "property_longitude-min",
                "property_tax-max",
                "property_tax-min",
                "riverine-max",
                "riverine-min",
                "lot_area-max",
                "lot_area-min",
            ]

            # Generate the query to get MIN and MAX for each numeric field
            min_max_query_parts = [
                f"MIN({field}) AS min_{field.lower()}, MAX({field}) AS max_{field.lower()}"
                for field in numeric_fields
            ]
            min_max_query = (
                f"SELECT {', '.join(min_max_query_parts)} FROM `{self.BIGQUERY_TABLE}`"
            )

            # Execute the query
            query_job = self._client.query(min_max_query)
            results = query_job.result()

            # Format the results
            formatted_results = {}
            for row in results:
                for field in numeric_fields:
                    field_lower = field.lower()
                    min_value = row[f"min_{field_lower}"]
                    max_value = row[f"max_{field_lower}"]

                    # Round to 3 decimal places if not an integer
                    if min_value is not None and not min_value.is_integer():
                        min_value = round(min_value, 3)
                    if max_value is not None and not max_value.is_integer():
                        max_value = round(max_value, 3)

                    if (
                        f"{field_lower}-min" not in skip_fields
                        or f"{field_lower}-max" not in skip_fields
                    ):
                        formatted_results[f"{field_lower}-min"] = min_value
                        formatted_results[f"{field_lower}-max"] = max_value

            # print(formatted_results)

            return formatted_results

        except Exception as e:
            print(f"Exception {e} while getting numeric filters")
            return None


class CustomQueryBuilder:
    def __init__(self, schema_file_name):
        with open(schema_file_name, "r") as f:
            self.schema = json.load(f) if schema_file_name else None

    def is_valid_schema(self):
        return isinstance(self.schema, list) and len(self.schema) > 0
    
    def _build_boolean_query(self, field_name, filter_values):
        if isinstance(filter_values, list):
            # Check if the list contains only 'ALL'
            if len(filter_values) == 1 and filter_values[0].upper() == "ALL":
                return None  # Or return an empty string ''
            elif len(filter_values) > 0:
                filter_values = [f"{str(val).upper()}" for val in filter_values]
                return f"{field_name} IN ({', '.join(filter_values)})"
        elif isinstance(filter_values, str):
            # Check if the string is 'ALL'
            if filter_values.upper() == "ALL":
                return None  # Or return an empty string ''
            return f"{field_name} = {filter_values.upper()}"
        else:
            raise ValueError(
                f"Invalid filter values format for STRING field {field_name}"
            )

    def build_query(self, field_name, filter_values):
        try:
            if not self.is_valid_schema():
                return ""

            # print('field_type')

            field_type = self._get_field_type(field_name)

            print(f"{field_name} {filter_values} {field_type}")
            if field_type == "STRING":
                return self._build_string_query(field_name, filter_values)
            elif field_type in ["INTEGER", "FLOAT"]:
                return self._build_numeric_query(field_name, filter_values)
            elif field_name == "zip-code-matching":
                return self._build_zip_code_matching_query(filter_values)
            elif field_type == 'BOOLEAN':
                return self._build_boolean_query(field_name, filter_values)
            # elif field_name == "owner-do-not-mail":
            #     return self._build_do_not_mail_query(field_name, filter_values)
            else:
                raise ValueError(
                    f"Invalid field name {field_name} filter val {filter_values}"
                )
        except Exception as e:
            print(f"Exception {e} while build query")
            traceback.print_exc()
            return None

    def _get_field_type(self, field_name):
        for field in self.schema:
            if field["name"] == field_name:
                return field["type"]
        return None

    def _build_string_query(self, field_name, filter_values):
        if isinstance(filter_values, list):
            # Check if the list contains only 'ALL'
            if len(filter_values) == 1 and filter_values[0].upper() == "ALL":
                return None  # Or return an empty string ''
            elif len(filter_values) > 0:
                filter_values = [f"'{str(val).upper()}'" for val in filter_values]
                return f"UPPER({field_name}) IN ({', '.join(filter_values)})"
        elif isinstance(filter_values, str):
            # Check if the string is 'ALL'
            if filter_values.upper() == "ALL":
                return None  # Or return an empty string ''
            return f"UPPER({field_name}) = '{filter_values.upper()}'"
        else:
            raise ValueError(
                f"Invalid filter values format for STRING field {field_name}"
            )

    def _build_numeric_query(self, field_name, filter_values):
        if isinstance(filter_values, dict):
            if "min" in filter_values and "max" in filter_values:
                if (
                    filter_values["min"] is not None
                    and filter_values["max"] is not None
                ):
                    min_value = filter_values["min"]
                    max_value = filter_values["max"]
                    return f"{field_name} BETWEEN {min_value} AND {max_value}"
                elif filter_values["min"] is not None and filter_values["max"] is None:
                    return f"{field_name} >= {filter_values['min']}"

                elif filter_values["min"] is None and filter_values["max"] is not None:
                    return f"{field_name} <= {filter_values['max']}"
                else:
                    return ""
            else:
                raise ValueError("Invalid filter values format for range query")
        elif isinstance(filter_values, list):
            a = []
            print("numeric elif")
            for val in filter_values:
                if val != "None" and str(val).isnumeric():
                    a.append(str(val))
                else:
                    a.append("null")

            sq = ",".join(a)
            return f"{field_name} in ({sq})"
            print(sq)
        else:
            raise ValueError(
                f"Invalid filter values format for numeric field {field_name}"
            )
        

    
    def _build_zip_code_matching_query(self, filter_values):
        if isinstance(filter_values, list):
            list_els = [str(i).strip().lower() for i in filter_values]
            if "all" in list_els or "select" in list_els:
                return ""

            else:
                if len(list_els) == 1:
                    if "true" in list_els:
                        return (
                           """(Property_Zip_Code = Mail_Zip_Code) """
                        
                        )
                    else:
                        return (
                           """(Property_Zip_Code != Mail_Zip_Code) """
                       
                          
                        )
                else:
                    return ""
        else:
            raise ValueError("Invalid filter values format for Zip_Code_Matching query")

    # def _build_zip_code_matching_query(self, filter_values):
    #     if isinstance(filter_values, list):
    #         list_els = [str(i).strip().lower() for i in filter_values]
    #         if "all" in list_els or "select" in list_els:
    #             return ""

    #         else:
    #             if len(list_els) == 1:
    #                 if "true" in list_els:
    #                     return (
    #                        """(Property_Zip_Code = Mail_Zip_Code """
    #                         + """OR Property_Zip_Code = Mail_Zip_Code_9 """
    #                         + """OR Property_Zip_Code = Mail_Zip_Code_R """
    #                         + """OR Property_Zip_Code = Mail_ZipCode_STD) """
    #                     )
    #                 else:
    #                     return (
    #                        """(Property_Zip_Code != Mail_Zip_Code """
    #                         + """AND Property_Zip_Code != Mail_Zip_Code_9 """
    #                         + """AND Property_Zip_Code != Mail_Zip_Code_R """
    #                         + """AND Property_Zip_Code != Mail_ZipCode_STD) """
    #                     )
    #             else:
    #                 return ""
    #     else:
    #         raise ValueError("Invalid filter values format for Zip_Code_Matching query")

    def _build_do_not_mail_query(self, field_name, filter_values):
        print("called--------",filter_values, type(filter_values))
        if filter_values == "True":
            return "EXISTS (SELECT 1 FROM `property-database-370200.Property_Dataset.Owners_Do_Not_Mail` As Owners_Do_Not_Mail WHERE JOINED.Owner_First_Name = Owners_Do_Not_Mail.name)"
        else:
            return ""
