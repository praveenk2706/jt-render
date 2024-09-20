import pandas as pd
import numpy as np
import logging
import traceback
import io
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import Resource

import time
from google.api_core.exceptions import Conflict
from concurrent.futures import ThreadPoolExecutor
class GoogleDriveFileReader:
    _SCOPE = ["https://www.googleapis.com/auth/drive"]

    _SPREADSHEET_ID = "1b4oKqlEFQAOf98lOpw9mkVcj8HLbN6ehLn89kOtVPtU"
    _SPREADSHEET_RANGE = ""
    EXPORTED_FILE_NAME = "pricing_research.xlsx"

    def __init__(self, processing_start_date=None) -> None:
        try:
            if processing_start_date is not None:
                self.processing_start_date = processing_start_date
            else:
                import datetime
                current_datetime = datetime.datetime.now()
                self.processing_start_date = current_datetime.strftime(
                    "%Y-%m-%dT%H:%M:%S"
                )

            # self.log_file_name = (
            #     f"/tmp/Log-GoogleDriveFileReader-{self.processing_start_date}.log"
            # )

            # Configure the root logger to save logs to a file
            # self.logger = logging.getLogger("GoogleDriveFileReader")
            # self.logger.setLevel(logging.DEBUG)
            # formatter = logging.Formatter(
            #     "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            # )
            # file_handler = logging.FileHandler(self.log_file_name)
            # file_handler.setFormatter(formatter)
            # self.logger.addHandler(file_handler)

            service_account_info_json = {
                "type": "service_account",
                "project_id": "mail-engine-411414",
                "private_key_id": "5e9b7613700a32da485204dea836088b67ef7ad2",
                "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC0pQQvB7MqrmLm\nTCNwwBwul21WMDb2b6K8Re28hWwrX4BYm055JNRwtnH1oNhLqj5H3ANa0DEMp3yy\nbyZAs0s5y7QKwfK1xGxtxXO+yaHMNGhTRDW1u3IpT/RYMiGqEUPHPbCiEIMiSsMb\nU+kQysC0EEXExST9jUa35AzqgMg5ublVCVMr2oj/3toSJe4/oDVQdLcMwGSYmi1V\np7MCt9HC+jCOoW3ArLSXUlsoO4G7c+Nz4Z6bX1SW9mlIe0fhFMF8aSzu1tdeOrNG\nqjvFhS3+sOp3lrbuZNX+HNgy3z0E8BbCCCsZ6A4qpDMwlHkCEy+vk2GeSYVJv+Ef\nq+IXP3xzAgMBAAECggEAGNZVgG05GQW4IGet5HCYOgm6T+7k0mEKtC3oiuf3YEag\nZs5tGDf++O5aNdu4fD17PwsaRPq6EROC5Xpl0os0V80mbeeOdP0oVrbJfB0iC2nV\n7EOadOhsa7oMDlz6MaAaZxhteE2ikhegbYSjLMVhKyt6REdCQkBnTZ0Omjw8YL8+\nLSNtR98Xb8dav87j2N/ouj+CI8PLUVTlr0U9thR4qVVy4J/irO9rYDvUvnqoWaT2\ncdctne1qDtROAiPAn2P2tvqLTugP4uU1NscHPVs85YW8yQt5IolYwlr4JaIfjFD1\n06p60m66e6jQOnGT5a6ggPtwxKS38XZ/8Tm/VYPyFQKBgQD5nr4XfVJYLizC5Mls\nGa1VtRBOInLa8wT9T883dQa0r6OhbtU3vWWJEelA0Sm9hyZBAs3pW+tULcXdhgfP\nKjPhjhfTw9wBO7GEOV/G91ElRo35QA5rzSqv57smB8xKy3p5HRu84b+PJD9Cpaf+\nH37B6LqCZthOP+TarXFEkX57FQKBgQC5QvgRtOUQg//kJdQk7nJe+tlDrQTlI3BU\nXnWVFJe8gomVJluUWqff12MeUXYPN7ybb0idI4ZkloWnR/2h8WuF8W9/ZTD+BiuY\n6xs/VzZaDh78B0oINFaaSzalGLyrKG6W9GvBkqJIqkekcq+T5JY2n+csk0Ylt6Di\n5NNmMg3bZwKBgHHQ2afoMnWWiD5NBzJM+uXLayXVOz1t9WZyz27f4zDbrOZcnMeP\nig5Xpl0xgbCzQNP/rVer279EJ6X8CO+CtKkxmtepxFSjnStG5c7Y0Z0HcJCnjmvE\n3qPaK1EJ2TJ5WNyEUzNqU2e3BUkkM1cBkVBlBzWSIyp6o6S51J2JIXyJAoGADqGz\nZfKbhMpoE2TJdIFAly/IqQepM0+xN3ieYL/XnBPTOexlnznpuEwSj/pvEJSeWMhZ\nO8/qdVdOBwAwj/G5RELlQ3KChA2Is/Mdm8sPh91FpTIOLsezb0wxbKiffgUbduCn\nAgrKtmJ8dgh4xX1wP7AxWdvn1mLCWikoaRHa9lECgYB9u29H9crqKv6+ww2ZJ3wZ\nCgll4Pwalq+f3rU/Kh4P5cC2X8y8TJksmbGod5NdE/kCwZ9RrhTkOF9lnbhs2HrI\nK57KlPQdxpJhElx5Tkg9Rrk+qy6pg3zD9AM6Ghe+O+wUeVK9/ed7naQXf/aUctqK\n1Kml/J5lguaH24uqB86sHg==\n-----END PRIVATE KEY-----\n",
                "client_email": "mail-engine-411414@appspot.gserviceaccount.com",
                "client_id": "116316020823060397699",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/mail-engine-411414%40appspot.gserviceaccount.com",
                "universe_domain": "googleapis.com",
            }

            # self.logger.debug("Creating service account credentials")
            self._credentials = service_account.Credentials.from_service_account_info(
                service_account_info_json
            )

            # self.logger.debug("Creating drive service")
            self._service = build("drive", "v3", credentials=self._credentials)
            # self.logger.debug(type(self._service))

        except Exception as e:
           # self.logger.error(f"Exception {str(e)} while creating drive service account")
            traceback.print_exc()

    def download_pricing_research_file_locally(self):
        try:
            service = build("drive", "v3", credentials=self._credentials)
            request = service.files().export_media(
                fileId=self._SPREADSHEET_ID,
                mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            with io.FileIO(self.EXPORTED_FILE_NAME, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    # self.logger.debug("Download %d%%." % int(status.progress() * 100))

        except Exception as e:
           # self.logger.error(f"Exception {str(e)}")
            traceback.print_exc()

    @property
    def is_drive_service_valid(self):
        return hasattr(self, "_service") and isinstance(self._service, Resource)


class PropertyRecordsUpdater:
    def __init__(self, service_account_info, project_id):
        self.service_account_info = service_account_info
        self._project_id = project_id
        self._client = None
        self._credentials = None
        # self.logger = self._setup_logger()
        self._create_connection_instance()
        self.drive_reader = GoogleDriveFileReader()

        self.drive_reader.download_pricing_research_file_locally()

        pricing_research_file_path = self.drive_reader.EXPORTED_FILE_NAME

        if (
            pricing_research_file_path is not None
            and isinstance(pricing_research_file_path, str)
            and len(pricing_research_file_path.strip()) > 0
            and Path(pricing_research_file_path).exists()
        ):
            self._pricing_research_file_path = pricing_research_file_path

            # Load pricing research data from Excel file
            pricing_excel_file = pd.ExcelFile(
                self._pricing_research_file_path, engine="openpyxl"
            )

            if not isinstance(pricing_excel_file, pd.ExcelFile):
               # self.logger.error("Cannot load pricing research")
                return None

            self._pricing_research_for_state = {}

            for sheet_name in pricing_excel_file.sheet_names:
                self._pricing_research_for_state[sheet_name] = pricing_excel_file.parse(
                    sheet_name=sheet_name,
                    dtype={
                        "State": str,
                        "County": str,
                        "Zip Code": str,
                    },
                )

                columns_to_convert = ["State", "County", "Zip Code", "APN Section"]
                self._pricing_research_for_state[sheet_name][columns_to_convert] = (
                    self._pricing_research_for_state[sheet_name][columns_to_convert].astype(str)
                )

        else:
            self._pricing_research_file_path = None

    def _create_connection_instance(self):
        try:
            if hasattr(self, "service_account_info"):
                try:
                    self._credentials = service_account.Credentials.from_service_account_info(
                        self.service_account_info
                    )
                    self._service = build("sheets", "v4", credentials=self._credentials)
                except Exception as e:
                    #print(f"Failed to create service account credentials: {e}")
                    self._credentials = None
                    self._client = None
                    return

                try:
                    self._client = bigquery.Client(
                        credentials=self._credentials, project=self._project_id
                    )
                except Exception as e2:
                    #print(f"Failed to create BigQuery client: {e2}")
                    self._client = None
            else:
                self._credentials = None
                self._client = None

        except Exception as e:
            #print(f"An unexpected error occurred while creating the connection: {e}")
            self._credentials = None
            self._client = None

    # def _setup_logger(self):
    #     logger = logging.getLogger("PropertyRecordsUpdater")
    #     logger.setLevel(logging.DEBUG)
    #     formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    #     file_handler = logging.FileHandler("/tmp/Log-PropertyRecordsUpdater.log")
    #     file_handler.setFormatter(formatter)
    #     logger.addHandler(file_handler)
    #     return logger

    def load_pricing_data(self):
        pricing_excel_file = pd.ExcelFile(self.drive_reader.EXPORTED_FILE_NAME, engine="openpyxl")
        pricing_data = {}
        for sheet_name in pricing_excel_file.sheet_names:
            pricing_data[sheet_name] = pricing_excel_file.parse(sheet_name)
        return pricing_data
    
    def _find_matching_row(
        self, pricing_data, state, county, zip_code, apn, lot_acreage, **kwargs
    ):
        #print(pricing_data)
        try:
            if (
                pricing_data is None
                or not isinstance(pricing_data, pd.DataFrame)
                or pricing_data.shape[0] == 0
            ):
                return None

            if str(zip_code).lower() in (
                "no information",
                "null",
                np.nan,
                None,
                "none",
                "nan",
            ):
                zip_code = np.nan

           
            # pricing_data["Zip Code"] = pricing_data["Zip Code"].astype(str)

            #print(str(zip_code), type(zip_code), type(pricing_data["Zip Code"][0]), pricing_data["Zip Code"])
            # Create match tables
            state_match = pricing_data["State"].str.lower() == str(state).lower()
            county_match = (
                pricing_data["County"].str.lower() == str(county).lower()
            ) | (pricing_data["County"].isnull())
            zip_code_match = (pricing_data["Zip Code"] == str(zip_code)) | (
                pricing_data["Zip Code"].isnull()
            )

            acreage_match = (
                (pricing_data["Starting Acreage"].fillna(0) <= lot_acreage)
                & (pricing_data["Ending Acreage"].fillna(0) >= lot_acreage)
            ) | (
                (pricing_data["Starting Acreage"].isnull())
                & (pricing_data["Ending Acreage"].isnull())
            )
            apn_section_match = (
                pricing_data["APN Section"].apply(
                    lambda x: str(apn).lower().startswith(str(x).lower().strip())
                )
            ) | (pricing_data["APN Section"].isnull())

            # Combine match tables
            match_sum = (
                state_match.astype(int)
                + county_match.astype(int)
                + zip_code_match.astype(int)
                + acreage_match.astype(int)
                + apn_section_match.astype(int)
            )

            # Find the row with the most True values (maximum match_sum)
            max_match_count = match_sum.max()
            matching_rows = pricing_data[match_sum == max_match_count]

            if matching_rows.empty:
                # self.logger.debug(
                #     f"No match found for {state}, {county}, {zip_code}, {apn}, {lot_acreage}"
                # )
                return None

            # Sort by DataFrame index if no specific criterion is available
            matching_rows = matching_rows.sort_index(ascending=False)

            # Select the row with the highest index
            best_match = matching_rows.iloc[0]

            return best_match

        except Exception as find_matching_rows_exc:
            # self.logger.warning(
            #     f"Exception {str(find_matching_rows_exc)} while finding matching rows for {state} {county} {zip_code} {apn} {lot_acreage} "
            # )
            traceback.print_exc()
            return None

    # Function to find price per acre and update Market Price
    def find_price_per_acre(self, row):
        """
        Find price per acre for a property and update Market Price.

        Args:
            row (Series): Row of data representing a property.

        Returns:
            Series: Updated row with Price_per_Acre and Market_Price columns.
        """

        try:
            
            # self.logger.debug(f"Processing row: {row.name}")
            # self.logger.debug(str(row["Property_State_Name_Short"]).strip().lower() if )
            sheet_name = ""

            # Map state abbreviation to corresponding sheet name
            match str(row["Property_State_Name"]).strip().lower():
                case "arizona":
                    sheet_name = "Arizona"
                case "georgia":
                    sheet_name = "Georgia"
                case "north carolina":
                    sheet_name = "NC"
                case _:
                    if str(row["Property_State_Name"]).strip().lower() in [
                        "NC",
                        "NORTH CAROLINA",
                        "NORTH-CAROLINA",
                        "NORTH_CAROLINA",
                        "North Carolina"
                    ]:
                        sheet_name = "NC"

            # self.logger.debug(f"{row['Property_State_Name']} {sheet_name}")

            pricing_data = self._pricing_research_for_state[sheet_name]

            # Extract pricing data for the state from Excel file
            # self.logger.debug("extract pricing data")

            # Check if pricing data is valid
            if (
                pricing_data is None
                or not isinstance(pricing_data, pd.DataFrame)
                or (
                    isinstance(pricing_data, pd.DataFrame)
                    and pricing_data.shape[0] <= 0
                )
            ):
                # self.logger.debug("pricing data invalid")
                # self.logger.debug(type(pricing_data))
                # self.logger.debug(pricing_data)
                return None

            state = ""

            # Map state abbreviation to corresponding state name abv
            match str(row["Property_State_Name"]).strip().lower():
                case "arizona":
                    state = "AZ"
                case "georgia":
                    state = "GA"
                case _:
                    if str(row["Property_State_Name"]).strip().upper() in [
                        "NC",
                        "NORTH CAROLINA",
                        "NORTH-CAROLINA",
                        "NORTH_CAROLINA",
                    ]:
                        state = "NC"

            county, zip_code, apn, lot_acreage = (
                row["Property_County_Name"].upper(),
                row["Property_Zip_Code"],
                row["APN"],
                row["Lot_Acreage"],
            )

            pricing_data = pricing_data[
                list(
                    set(pricing_data.columns)
                    & set(
                        [
                            "State",
                            "County",
                            "Zip Code",
                            "APN Section",
                            "Starting Acreage",
                            "Ending Acreage",
                            "Per Acre Pricing - Value",
                        ]
                    )
                )
            ]

            # Find matching row in pricing data (considering order)
            matched_row = self._find_matching_row(
                pricing_data=pricing_data,
                state=state,
                county=county,
                zip_code=zip_code,
                apn=apn,
                lot_acreage=lot_acreage,
                owner_id=row["Owner_ID"],
            )

            # self.logger.debug("--- matched row ----")

            # check if matched_row is a valid pandas series
            # with a valid Per Acre
            if (
                matched_row is not None
                and isinstance(matched_row, pd.Series)
                and "Per Acre Pricing - Value" in matched_row.index
                and not pd.isna(matched_row["Per Acre Pricing - Value"])
                and isinstance(
                    matched_row["Per Acre Pricing - Value"],
                    (
                        int,
                        float,
                        np.int64,
                        np.float64,
                    ),
                )
                and matched_row["Per Acre Pricing - Value"] > 0
            ):
                # self.logger.debug("----matched_row is valid----")

                row["Price_per_Acre"] = round(
                    matched_row["Per Acre Pricing - Value"], 2
                )
            else:
                # self.logger.debug(
                #     f"the calculation of pricing per acre from pricing research csv was inconclusive {state} {county} {apn} {zip_code} {lot_acreage} {type(matched_row)}"
                # )
                # self.logger.debug(type(matched_row))
                # if isinstance(matched_row, pd.Series):
                #     self.logger.debug("Per Acre Pricing - Value" in matched_row.index)
                #     self.logger.debug(not pd.isna(matched_row["Per Acre Pricing - Value"]))
                #     self.logger.debug(type(matched_row["Per Acre Pricing - Value"]))

                # writeable = (
                #     f"{row['Owner_ID']},{state},{county},{zip_code},{apn},{lot_acreage}"
                # )

                # with open(self.missed_properties_csv, "+a") as f:
                #     f.write(writeable + "\n")
                #     f.close()

                # exit(0)

                # self.logger.debug(type(matched_row))
                row["Price_per_Acre"] = 0.00

            # Update Market Price
            row = self.update_market_price(row)

            
            return row

        except Exception as find_price_per_acre_exception:
           # self.logger.error(
            #     f"Exception while calculating price per acre {str(find_price_per_acre_exception)}"
            # )
            traceback.print_exc()

        finally:
            return row
    def update_market_price(self, row):
        """
        Update Margenerate individual pdfs and push the to google storage
        push the individual user pdf to letter collection of mango dbket Price based on Price per Acre and Lot Acreage.

        Args:
            row (Series): Row of data representing a property.

        Returns:
            Series: Updated row with Market_Price column.
        """

        try:
            if (
                not pd.isna(row["Price_per_Acre"])
                and isinstance(
                    row["Price_per_Acre"], (int, float, np.int64, np.float64)
                )
                and row["Price_per_Acre"] != 0
                and not pd.isna(row["Lot_Acreage"])
                and isinstance(row["Lot_Acreage"], (int, float, np.int64, np.float64))
                and row["Lot_Acreage"] != 0
            ):
                # Calculate updated Market Price based on Price per Acre and Lot Acreage
                row["Market_Price"] = round(
                    row["Price_per_Acre"] * row["Lot_Acreage"], 2
                )

        except Exception as update_market_price_exception:
           # self.logger.error(
            #     f"Exception while updating market price {str(update_market_price_exception)}"
            # )
            traceback.print_exc()

        finally:
            return row


    def update_bigquery(self, dataframe, table_id):
        max_retries = 3
        delay = 5  # seconds

        for attempt in range(max_retries):
            try:
                # Ensure unique rows in the dataframe
                dataframe = dataframe.drop_duplicates(subset=['Owner_ID'])
                
                # Create a temporary table for staging updates
                temp_table_id = f"{table_id}_temp"
                dataframe.to_gbq(destination_table=temp_table_id, project_id=self._project_id, if_exists='replace')

                # Use a BigQuery SQL query to update the target table from the temporary table
                query = f"""
                MERGE `{table_id}` T
                USING `{temp_table_id}` S
                ON T.Owner_ID = S.Owner_ID
                WHEN MATCHED THEN
                UPDATE SET T.Market_Price = S.Market_Price
                """

                self._client.query(query).result()  # Wait for the query to finish
                #print(f"Updated market prices from temporary table to {table_id}")

                # Clean up the temporary table
                self._client.query(f"DROP TABLE `{temp_table_id}`").result()
                return  # Exit the function if successful

            except Conflict as e:
                #print(f"Conflict encountered. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff

            except Exception as e:
               # self.logger.error(f"Error updating BigQuery: {str(e)}")
                traceback.print_exc()
                break  # Exit the loop on other errors



    def process_and_update(self, table_id):
        try:
            batch_size = 1000
            offset =  1141000
            total_processed = 0

            while True:
                # Modify the query to include LIMIT and OFFSET for batch processing
                query = f"""
                SELECT DISTINCT Owner_ID, Property_State_Name, Property_County_Name, Property_Zip_Code, APN, Lot_Acreage
                FROM `{table_id}`
                ORDER BY Owner_ID, Property_State_Name, Property_County_Name, Property_Zip_Code, APN, Lot_Acreage
                LIMIT {batch_size} OFFSET {offset}
                """
                
                dataframe = self._client.query(query).to_dataframe()
                if dataframe.empty:
                    break  # Exit if there are no more rows to process

                print(f"Processing batch {offset // batch_size + 1}, {len(dataframe)} records...")

                with ThreadPoolExecutor(max_workers=6) as executor:
                    futures = [executor.submit(self.find_price_per_acre, row) for idx, row in dataframe.iterrows()]
                    updated_rows = [future.result() for future in futures]

                # Filter out None rows and prepare the DataFrame for batch update
                updated_dataframe = pd.DataFrame([row for row in updated_rows if row is not None])

                if not updated_dataframe.empty:
                    self.update_bigquery(updated_dataframe, table_id)

                # Update offset for the next batch
                total_processed += len(dataframe)
                offset += batch_size

                # If the returned dataframe size is less than the batch_size, it's the last batch
                if len(dataframe) < batch_size:
                    break

            print(f"Total records processed: {total_processed}")

        except Exception as e:
            # self.logger.error(f"Error processing records: {str(e)}")
            traceback.print_exc()



# Main entry point
if __name__ == "__main__":
    # Add actual service account info and project id
    service_account_info = {
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
    project_id = 'property-database-370200'
    table_id = f'{project_id}.Dataset_v3.Properties_New'
    updater = PropertyRecordsUpdater(service_account_info, project_id)
    updater.process_and_update(table_id)
