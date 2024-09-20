import datetime
import io
import logging
import random
import traceback
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError  # noqa: F401
from googleapiclient.http import MediaIoBaseDownload


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
                current_datetime = datetime.datetime.now()

                self.processing_start_date = current_datetime.strftime(
                    "%Y-%m-%dT%H:%M:%S"
                )

            self.log_file_name = (
                f"/tmp/Log-GoogleDriveFileReader-{self.processing_start_date}.log"
            )

            # Configure the root logger to save logs to a file
            self.logger = logging.getLogger("GoogleDriveFileReader")
            self.logger.setLevel(logging.DEBUG)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            file_handler = logging.FileHandler(self.log_file_name)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

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

            self.logger.debug("creating service account credentials")

            self._credentials = service_account.Credentials.from_service_account_info(
                service_account_info_json
            )

            self.logger.debug("creating drive service")
            self._service = build("sheets", "v4", credentials=self._credentials)
            self.logger.debug(type(self._service))

        except Exception as e:
            self.logger.error(
                f"Exception {str(e)} while creating drive service account"
            )
            traceback.print_exc()

    def download_pricing_research_file_locally(self):
        try:
            service = build("drive", "v3", credentials=self._credentials)

            request = service.files().export_media(
                fileId=self._SPREADSHEET_ID,
                mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            # fh = io.FileIO(self.EXPORTED_FILE_NAME, "wb")
            fh = io.FileIO(self.EXPORTED_FILE_NAME, "wb")

            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                self.logger.debug("Download %d%%." % int(status.progress() * 100))

        except Exception as e:
            self.logger.error(f"Exception {str(e)}")
            traceback.print_exc()

    @property
    def is_drive_service_valid(self):
        if (
            hasattr(self, "_service")
            and self._service is not None
            and isinstance(self._service, Resource)
        ):
            return True
        else:
            return False


class PropertyRecordsPreProcessor:
    # Define the mapping of group numbers for each state and property count
    group_number_mapping = {
        "AZ": {"1": "014", "2": "015", "3": "016", "4": "017", "5": "017"},
        "GA": {"1": "021", "2": "022", "3": "023", "4": "024", "5": "025"},
        "NC": {"1": "032", "2": "033", "3": "034", "4": "035", "5": "036"},
    }

    # Dictionary to store used control numbers for each group
    used_control_numbers = {}

    _state_abrevs = {
        "arizona": "AZ",
        "georgia": "GA",
        "north carolina": "NC",
        "north-carolina": "NC",
        "north_carolina": "NC",
    }

    _fields_to_include = [
        "Owner_ID",
        "Owner_First_Name",
        "Owner_Last_Name",
        "Owner_Full_Name",
        "Owner_Short_Name",
        "Owner_Name_Type",
        "Property_State_Name",
        "Mail_Street_Address",
        "Mail_City",
        "Mail_State",
        "Mail_Zip_Code",
        "APN",
        "Lot_Acreage",
        "Property_State_Name",
        "Property_County_Name",
        # "Property_State_Name_Short",
        "Property_Zip_Code",
        # "Property_City",
        # "Property_Address_Full",        
        # "Owner_2_First_Name",
        # "Owner_2_Last_Name",
        # "Owner_Type",
        # "Market_Price",
        # "Date",
        # "Mail_Address_Full",
        # "Owner_Mailing_Name",
    ]

    def __init__(self, dataframe, processing_start_date=None) -> None:
        if (
            dataframe is not None
            and isinstance(dataframe, pd.DataFrame)
            and dataframe.shape[0] > 0
        ):
            if processing_start_date is not None:
                self.processing_start_date = processing_start_date
            else:
                current_datetime = datetime.datetime.now()

                self.processing_start_date = current_datetime.strftime(
                    "%Y-%m-%dT%H:%M:%S"
                )

            self.missed_properties_csv = (
                f"/tmp/missed_properties-{self.processing_start_date}.csv"
            )
            writeable = f"Owner_ID,State,County,Zip Code,APN,Lot Acreage\n"

            with open(self.missed_properties_csv, "+a") as f:
                f.write(writeable)
                f.close()

            self.log_file_name = (
                f"/tmp/Log-PropertyRecordsPreProcessor-{self.processing_start_date}.log"
            )
            # Configure the root logger to save logs to a file
            self.logger = logging.getLogger("PropertyRecordsPreProcessor")
            self.logger.setLevel(logging.DEBUG)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            file_handler = logging.FileHandler(self.log_file_name)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            self._dataframe = dataframe

            self._dataframe = self._dataframe.drop(
                self._dataframe.columns.difference(self._fields_to_include), axis=1
            )

            self._title_case_columns = [
                "Property_State_Name",
                "Property_County_Name",
                # "Owner_First_Name",
                # "Owner_Last_Name",
                # "Owner_Full_Name",
                # "Owner_Mailing_Name",
                # "Property_City",
            ]

            self._upper_case_columns = [
                "APN",
                # "Property_State_Name_Short",
                "Owner_ID",
            ]

            self._dataframe[self._title_case_columns] = self._dataframe[
                self._title_case_columns
            ].applymap(lambda x: str(x).title() if pd.notna(x) else "")

            self._dataframe[self._upper_case_columns] = self._dataframe[
                self._upper_case_columns
            ].applymap(lambda x: str(x).upper() if pd.notna(x) else "")

        else:
            self._dataframe = None

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

            # Check if pricing research data is successfully loaded
            if not isinstance(pricing_excel_file, pd.ExcelFile):
                self.logger.error("cannot load pricing research")
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

                # Convert specified columns to strings
                columns_to_convert = ["State", "County", "Zip Code", "APN Section"]
                self._pricing_research_for_state[sheet_name][columns_to_convert] = (
                    self._pricing_research_for_state[
                        sheet_name
                    ][columns_to_convert].astype(str)
                )

        else:
            self._pricing_research_file_path = None

    @property
    def dataframe(self):
        if (
            self._dataframe is not None
            and isinstance(self._dataframe, pd.DataFrame)
            and self._dataframe.shape[0] > 0
        ):
            return self._dataframe.head()

        else:
            return type(self._dataframe)

    @property
    def is_valid_dataframe(self):
        return (
            self._dataframe is not None
            and isinstance(self._dataframe, pd.DataFrame)
            and self._dataframe.shape[0] > 0
        )

    @property
    def is_valid_pricing_research_file(self):
        if (
            hasattr(self, "_pricing_research_file_path")
            and self._pricing_research_file_path is not None
            and isinstance(self._pricing_research_file_path, str)
            and len(self._pricing_research_file_path.strip()) > 0
            and Path(self._pricing_research_file_path).exists()
        ):
            return True
        else:
            return False

    def _find_matching_row(
        self, pricing_data, state, county, zip_code, apn, lot_acreage, **kwargs
    ):
        print(pricing_data)
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
                self.logger.debug(
                    f"No match found for {state}, {county}, {zip_code}, {apn}, {lot_acreage}"
                )
                return None

            # Sort by DataFrame index if no specific criterion is available
            matching_rows = matching_rows.sort_index(ascending=False)

            # Select the row with the highest index
            best_match = matching_rows.iloc[0]

            return best_match

        except Exception as find_matching_rows_exc:
            self.logger.warning(
                f"Exception {str(find_matching_rows_exc)} while finding matching rows for {state} {county} {zip_code} {apn} {lot_acreage} "
            )
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
            
            self.logger.debug(f"Processing row: {row.name}")
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

            self.logger.debug(f"{row['Property_State_Name']} {sheet_name}")

            pricing_data = self._pricing_research_for_state[sheet_name]

            # Extract pricing data for the state from Excel file
            self.logger.debug("extract pricing data")

            # Check if pricing data is valid
            if (
                pricing_data is None
                or not isinstance(pricing_data, pd.DataFrame)
                or (
                    isinstance(pricing_data, pd.DataFrame)
                    and pricing_data.shape[0] <= 0
                )
            ):
                self.logger.debug("pricing data invalid")
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

            self.logger.debug("--- matched row ----")

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
                self.logger.debug("----matched_row is valid----")

                row["Price_per_Acre"] = round(
                    matched_row["Per Acre Pricing - Value"], 2
                )
            else:
                self.logger.debug(
                    f"the calculation of pricing per acre from pricing research csv was inconclusive {state} {county} {apn} {zip_code} {lot_acreage} {type(matched_row)}"
                )
                # self.logger.debug(type(matched_row))
                # if isinstance(matched_row, pd.Series):
                #     self.logger.debug("Per Acre Pricing - Value" in matched_row.index)
                #     self.logger.debug(not pd.isna(matched_row["Per Acre Pricing - Value"]))
                #     self.logger.debug(type(matched_row["Per Acre Pricing - Value"]))

                writeable = (
                    f"{row['Owner_ID']},{state},{county},{zip_code},{apn},{lot_acreage}"
                )

                with open(self.missed_properties_csv, "+a") as f:
                    f.write(writeable + "\n")
                    f.close()

                # exit(0)

                # self.logger.debug(type(matched_row))
                row["Price_per_Acre"] = 0.00

            # Update Market Price
            row = self.update_market_price(row)

            row["Offer_Percentage"] = self.calculate_offer_percentage(
                market_price=row["Market_Price"]
            )
            row["Offer_Price"] = self.calculate_offer_price(
                market_price=row["Market_Price"],
                offer_percentage=row["Offer_Percentage"],
            )
            return row

        except Exception as find_price_per_acre_exception:
            self.logger.error(
                f"Exception while calculating price per acre {str(find_price_per_acre_exception)}"
            )
            traceback.print_exc()

        finally:
            return row

    # Function to update Market Price based on Price per Acre and Lot Acreage
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
            self.logger.error(
                f"Exception while updating market price {str(update_market_price_exception)}"
            )
            traceback.print_exc()

        finally:
            return row

    # Function for offer percentage based on market price
    def calculate_offer_percentage(self, market_price):
        if market_price < 50000:
            return 0.4
        elif market_price < 100000:
            return 0.5
        else:
            return 0.55

    # Function for offer price based on market price
    def calculate_offer_price(self, market_price, offer_percentage):
        return round(market_price * offer_percentage, 2)

    def process_batch(self, df_batch):
        """
        Function to process a batch of data by applying find_price_per_acre function.

        Args:
            df_batch (DataFrame): Batch of data to process.

        Returns:
            DataFrame: Processed batch with updated price per acre.
        """
        self.logger.debug("processing batch")
        processed_batch = df_batch.apply(self.find_price_per_acre, axis=1)

        # processed_batch["Offer_Price_Randomization"] = np.random.uniform(
        #     1.01, 99.99, len(processed_batch)
        # )
        # processed_batch["Final_Offer_Price"] = (
        #     processed_batch["Offer_Price"]
        #     + processed_batch["Offer_Price_Randomization"]
        # )
        # processed_batch["Offer_Price_Randomization"] = processed_batch["Offer_Price_Randomization"].round(2)
        # processed_batch["Final_Offer_Price"] = processed_batch["Final_Offer_Price"].round(2)

        return processed_batch

    def split_and_process_dataframe_into_mail_groups(self, df):
        try:
            # Step 1: Filter the DataFrame
            df = df[df["Property_State_Name"].str.lower().isin(["arizona", "georgia"])]

            # Step 2: Group the Data (case-insensitive)
            df["Property_State_Name"] = df["Property_State_Name"].str.title()
            df["Property_County_Name"] = df["Property_County_Name"].str.title()
            grouped = df.groupby(
                ["Owner_ID", "Property_State_Name", "Property_County_Name"],
                as_index=False,
            )

            # Step 3: Shuffle the Groups
            groups = list(grouped)
            np.random.shuffle(groups)

            # Step 4: Divide into State Batches
            arizona_groups = [g for g in groups if g[0][1] == "Arizona"]
            georgia_groups = [g for g in groups if g[0][1] == "Georgia"]

            # Step 5: Split into National and Local Brands
            national_arizona, local_arizona = (
                arizona_groups[: len(arizona_groups) // 2],
                arizona_groups[len(arizona_groups) // 2 :],
            )
            national_georgia, local_georgia = (
                georgia_groups[: len(georgia_groups) // 2],
                georgia_groups[len(georgia_groups) // 2 :],
            )

            # Step 6: Split into Contract Front and Contract Back
            def split_contract(groups):
                half = len(groups) // 2
                return groups[:half], groups[half:]

            national_arizona_front, national_arizona_back = split_contract(
                national_arizona
            )
            local_arizona_front, local_arizona_back = split_contract(local_arizona)
            national_georgia_front, national_georgia_back = split_contract(
                national_georgia
            )
            local_georgia_front, local_georgia_back = split_contract(local_georgia)

            # Step 7: Assign Control Numbers
            def assign_control_numbers(batch):
                population_size = 999999
                batch_size = len(batch)
                sample_size = min(population_size, batch_size)
                control_numbers = random.sample(
                    range(1, max(population_size, batch_size)), sample_size
                )
                return control_numbers

            # Define Mail Group Batch info
            mail_group_info = {
                "national_arizona_front": {
                    "name": "AZ017",
                    "type": "SLI & Contract Front",
                    "phone": "(520) 353-1257",
                },
                "national_arizona_back": {
                    "name": "AZ018",
                    "type": "SLI & Contract Back",
                    "phone": "(520) 337-9208",
                },
                "local_arizona_front": {
                    "name": "AZ015",
                    "type": "AZLF & Contract Front",
                    "phone": "(520) 597-7886",
                },
                "local_arizona_back": {
                    "name": "AZ016",
                    "type": "AZLF & Contract Back",
                    "phone": "(520) 277-0460",
                },
                "national_georgia_front": {
                    "name": "GA026",
                    "type": "SLI & Contract Front",
                    "phone": "(912) 513-4454",
                },
                "national_georgia_back": {
                    "name": "GA027",
                    "type": "SLI & Contract Back",
                    "phone": "(678) 944-7634",
                },
                "local_georgia_front": {
                    "name": "GA024",
                    "type": "GLI & Contract Front",
                    "phone": "(912) 455-7202",
                },
                "local_georgia_back": {
                    "name": "GA025",
                    "type": "GLI & Contract Back",
                    "phone": "(912) 900-1389",
                },
            }

            # Step 8: Update Batches with Additional Info and Control Numbers
            def update_batch(batch, info):
                control_numbers = assign_control_numbers(batch)
                updated_batch = []
                for (key, df), control_number in zip(batch, control_numbers):
                    df["brand_name"] = info["brand_name"]
                    df["website"] = info["website"]
                    df["email"] = info["email"]
                    df["phone"] = info["phone"]
                    df["Mailer_Group"] = info["name"]
                    df["type"] = info["type"]
                    df["signature"] = "signature.png"
                    df["control_number"] = control_number
                    df["Property_State_Name"] = df["Property_State_Name"].str.title()
                    df["Property_County_Name"] = df["Property_County_Name"].str.title()
                    updated_batch.append(df)
                return updated_batch

            # Assuming the conditions to check if a group is National or Local, these are placeholders
            brand_details = {
                "national": {
                    "brand_name": "Sunset Land Investors",
                    "website": "SunsetLandInvestors.com",
                    "email": "Contact@SunsetLandInvestors.com",
                },
                "local_georgia": {
                    "brand_name": "Georgia Land Investors",
                    "website": "www.GeorgiaLandInvestors.com",
                    "email": "Contact@GeorgiaLandInvestors.com",
                },
                "local_arizona": {
                    "brand_name": "Arizona Land & Farm",
                    "website": "www.ArizonaLandandFarm",
                    "email": "Contact@ArizonaLandandFarm.com",
                },
            }

            batches = []
            for key, groups in zip(
                [
                    "national_arizona_front",
                    "national_arizona_back",
                    "local_arizona_front",
                    "local_arizona_back",
                    "national_georgia_front",
                    "national_georgia_back",
                    "local_georgia_front",
                    "local_georgia_back",
                ],
                [
                    national_arizona_front,
                    national_arizona_back,
                    local_arizona_front,
                    local_arizona_back,
                    national_georgia_front,
                    national_georgia_back,
                    local_georgia_front,
                    local_georgia_back,
                ],
            ):
                if "national" in key:
                    info = {**mail_group_info[key], **brand_details["national"]}
                elif "georgia" in key:
                    info = {**mail_group_info[key], **brand_details["local_georgia"]}
                else:
                    info = {**mail_group_info[key], **brand_details["local_arizona"]}
                batches.extend(update_batch(groups, info))

            # Combine all DataFrames in batches
            combined_df = pd.concat(batches)

            return combined_df

        except Exception as e:
            print(e)
            traceback.print_exc()
            return None

    def pre_process_fetched_results(self):
        """
        Pre-process fetched results by finding the price per acre for each property and updating the Market Price.

        Returns:
            DataFrame: Pre-processed DataFrame with updated Price_per_Acre and Market_Price columns.
        """
        try:
            # Check if the dataframe is valid
            if not self.is_valid_dataframe:
                # Print information about the dataframe if it's invalid
                if isinstance(self._dataframe, pd.DataFrame):
                    self.logger.debug(self._dataframe.shape)
                else:
                    self.logger.debug(type(self._dataframe))

                # Return error message if the dataframe is invalid
                return {"message": "failed", "error": "passed dataframe is invalid"}

                # Define function to find price per acre

            # Add 'Price_per_Acre' column with default value
            self._dataframe["Price_per_Acre"] = np.nan
            self._dataframe["Market_Price"] = 0

            if not self.is_valid_pricing_research_file:
                return {
                    "message": "failed",
                    "error": "passed pricing research file is invalid",
                }

            # Split DataFrame into four equal-sized batches
            batch_size = self._dataframe.shape[0] // 7
            batches = [
                self._dataframe.iloc[i : i + batch_size]
                for i in range(0, self._dataframe.shape[0], batch_size)
            ]

            result_batches = []

            # Create a ProcessPoolExecutor
            with ProcessPoolExecutor() as executor:
                # Submit tasks to the executor
                futures = [
                    executor.submit(self.process_batch, batch) for batch in batches
                ]

                # Gather results
                result_batches = [future.result() for future in futures]

            # Combine results from each batch into a single DataFrame
            self._dataframe = pd.concat(result_batches)

            self.logger.debug("price per acre complete")

            return self._dataframe
            self.logger.debug("Randomly breaking the dataframe into testing groups")

            print(self._dataframe.head(1).to_dict(orient="records"))

            self._dataframe.to_csv("test_sample_pricing.csv", index=False)

            exit(0)

            updated_batches = self._dataframe

            # print(type(self._dataframe))

            # updated_batches = self.split_and_process_dataframe_into_mail_groups(
            #     df=self._dataframe
            # )

            file_names = []
            signed_urls = []
            upload_locations = []

            print("Random breaking done")

            # print(len(updated_batches))
            # print(f"{type(updated_batches)}")

            if (
                updated_batches is not None
                and isinstance(updated_batches, pd.DataFrame)
                and updated_batches.shape[0] > 0
            ):
                print("updated batches is good")

                # Get the current date in yyyy-mm-dd format
                current_date = datetime.datetime.now().strftime("%Y-%m-%d")

                for mailer_group, group_df in updated_batches.groupby("Mailer_Group"):
                    file_name = f"{mailer_group}-{current_date}.csv"

                    file_path = f"/tmp/{file_name}"
                    group_df.to_csv(file_path, index=False)
                    print(f"Exported DataFrame to {file_name}")

                    # if mailer_group == "GA027":
                    #     print(group_df.head().to_dict())

                    if Path(file_path).exists():
                        file_names.append(file_path)

            else:
                print("cannot use updated batches")

            print(file_names)

            # if (
            #     file_names is not None
            #     and isinstance(file_names, list)
            #     and len(file_names) > 0
            # ):
            #     for index, file_path in enumerate(file_names):
            #         # if index >= 1:
            #         # break
            #         print(f"File {file_path} {index} upload step")

            #         new_file_name = str(file_path).strip("./").strip("/tmp/")

            #         relative_path = Path(file_path)
            #         if Path(file_path).exists():
            #             absolute_path_string = str(relative_path.resolve())

            #             # upload_result = self.upload_file_to_cloud_storage_bucket(
            #             #     file_name=new_file_name, file_path=absolute_path_string
            #             # )

            #             upload_result = None

            #             if (
            #                 upload_result is not None
            #                 and isinstance(upload_result, str)
            #                 and len(upload_result.strip()) > 0
            #             ):
            #                 print(f"Upload result {file_path} {upload_result}")
            #                 upload_locations.append(upload_result.strip())

            #                 self.logger.debug(
            #                     "exported manipulated version, return success status to initiate email notification"
            #                 )

            #                 print(f"Upload locations {file_path}")
            #                 print(f"creating signed url for {file_path}")

            #                 signed_url = self.create_signed_url(file_name=new_file_name)

            #                 signed_urls.append(signed_url)

            #                 print(
            #                     f"deleting the exported file for {file_path} {file_path}"
            #                 )

            #                 file_names.append(file_path)

            #                 Path(relative_path.resolve()).unlink()

            def find_missed_properties_files(directory="/tmp"):
                try:
                    # Define the path to the directory
                    path = Path(directory)

                    # Find all csv files starting with "missed_properties"
                    csv_files = path.glob("missed_properties*.csv")

                    # Create a list of tuples with absolute path and file name
                    result = [(str(file.resolve()), file.name) for file in csv_files]

                    return result

                except Exception as e:
                    print(f"Exception {e} while uploading missed properties")
                    traceback.print_exception()

                    return None

            # upload the missed properties.
            missed_properties = find_missed_properties_files()
            if (
                missed_properties is not None
                and isinstance(missed_properties, list)
                and len(missed_properties) > 0
                and all(
                    isinstance(item, tuple)
                    and len(item) == 2
                    and all(isinstance(element, str) for element in item)
                    for item in missed_properties
                )
            ):
                self.logger.debug(
                    "Validation successful. All missed properties file items are correctly formatted."
                )

                # for index, item in enumerate(missed_properties):
                #     self.logger.debug(
                #         f"Upload missed properties csv {item} at index {index}"
                #     )

                #     (
                #         missed_properties_file_path_absolute_string,
                #         missed_properties_file_name,
                #     ) = (item[0], item[1])

                #     missed_properties_upload_result = (
                #         self.upload_file_to_cloud_storage_bucket(
                #             file_name=missed_properties_file_name,
                #             file_path=missed_properties_file_path_absolute_string,
                #         )
                #     )

                #     print(
                #         f"Missed properties upload result {missed_properties_upload_result}"
                #     )

                def find_log_files(directory="/tmp"):
                    try:
                        # Define the path to the directory
                        path = Path(directory)

                        # Find all log files
                        log_files = path.glob("*.log")

                        # Create a list of tuples with absolute path and file name
                        result = [
                            (str(file.resolve()), file.name) for file in log_files
                        ]

                        return result

                    except Exception as e:
                        print(f"Exception {e} while searching for log files")
                        traceback.print_exception()

                        return None

                # Find the log files
                log_files = find_log_files()

                # Validate the result
                if (
                    log_files is not None
                    and isinstance(log_files, list)
                    and len(log_files) > 0
                    and all(
                        isinstance(item, tuple)
                        and len(item) == 2
                        and all(isinstance(element, str) for element in item)
                        for item in log_files
                    )
                ):
                    print(
                        "Validation successful. All log file items are correctly formatted."
                    )
                    # Iterate over the list and process the log files
                    for log_file in log_files:
                        log_file_absolute_path, log_file_name = log_file
                        print(
                            f"Log file: {log_file_name}, Absolute path: {log_file_absolute_path}"
                        )
                        upload_log_file_result = (
                            self.upload_log_to_cloud_storage_bucket(
                                file_name=log_file_name,
                                file_path=log_file_absolute_path,
                            )
                        )

                        print(f"Log file upload result {upload_log_file_result}")
                else:
                    print(
                        "Validation failed. The returned result is not correctly formatted."
                    )

            return {
                "message": "success",
                "file_names": file_names,
                "storage_locations": upload_locations,
                "download_urls": signed_urls,
            }

        except Exception as preprocessing_exception:
            self.logger.error(f"Exception {str(preprocessing_exception)}")
            traceback.print_exc()

            return {"message": "failed", "error": str(preprocessing_exception)}

    def create_cloud_storage_client(self):
        """
        Creates a Google Cloud Storage client for handling file uploads and downloads.
        """
        try:
            self._storage_service_account_info = {
                "type": "service_account",
                "project_id": "mail-engine-411414",
                "private_key_id": "a7ad4ee8f14e0776b76aa73a94e7bc584e363b1c",
                "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDOFQ58wLHF9Hve\nqRgPjy2+3aHExea5E5m3pczHpdx/FbDZE/CTI80DYy1eXejH2WPCOAfYh8JWqiB0\n429KGJm17hBGjgrU40VxwGbKFiEUzZFwDDSoOyMNzmr1umMcZoVYZu5fhogmksoU\neaYgzwYLbtUtN5MVhG5bjG3odaYasl1VCxbnq4vlnDr6v1iaPCUZlwyP4ZpPBTdU\nrMrLyLDE4hym4kXjrRtMa5ziALqmDtIiFPqb56QJv/Y+AqOfLpOmZbreq6NmYmoO\nRq+dUCr2VsPIUwVlUlEj9mdEbFds/eLSjJBgspg1OzEDLr2knJIypEl8yxey6O7O\nqIaHSABTAgMBAAECggEAGJqx+poE/4Xjfh+BJGZrR0TtyfD4zXL2INN2QgXe7/tW\nfGxhhie5k+R057Xdk3K0ct0+ro7y2GcCbgIzaMzMGUj9f3b4+xbRVTXr78e5lmK+\nsPp8FPPOiRjyczkYGS1yUS4k2pnUyoou/0fQ2kztQ1Mtt3LRqDmB9YMsIhtdxAT9\nvj8fpWUInG/x2PQsSUG3BCNmdud1TP6KiLJBorRcsPkFY7UdOHO1FDKZVoMCAgdj\nhU/VsePP31evriFhsbQSHIaopKpj46Z3mMLnL0YC8dzZE6JFP7ubB5UrfmHQ6C0o\ntxb6SK5Qc7oSwleVtEkg7PyvAKwZ3XrfBkIY1atLvQKBgQDpXXkgEZ5+iGi0wZ/r\n1aZ0ELeYAo7Iw/ZJ5HwATup3VQx0gYMiv2Vymnf/BPLxp360yrl81AqUBx9aO868\nZDFD/CwFVR24fxg88fUzmn99MLPOYFcPm+V/dlrYGRfHA1heKleO6ifZ0RxqeLjm\nWR3dQFjBcRGPSzpOeTGpdo0+lQKBgQDiEiQu7hvn5zgg0VuVv8qi45f/EatYllWx\n7unMiK3jJjJE1xP0SamMqLxa74tfHloDoof4i/UFC3WRh1AYZwTXowCYQrxXn4tf\n5b6/Qa0GknTYTenF2u8PT+4i54/Odk5gxh8B1YjkDEGBgln2xgtznyfaYn0ieGB2\npa8q8PTRRwKBgQC1Nbo6prPWOYJk6f2Omca5VcnaphHN4C09T8jiTGVQ6J/VUERO\nFSVnin8nbeZgs9l19f126wiTzbwQy2RLcdm3mvdr5J5Bh5+Ao7ntqkjZI5pb5P91\nxW6+PV9pcoK+LODPrj1zYmwzfWd9XeTmKclA5xiEjZI/HQFXslBX8RDYyQKBgHoc\n4n6XD2vMcX0ImTfiSUqDNW8J1EwdqFxceN2KVqMD1tNoedbSk0rvOg7EpbvTYTvl\nZxzSPJ9k+TSKqrEDtJHl8kRMh/+splQ+fTJB/3w1T/gm3ceJ4ueafT9NZVhYfy22\n1rje5vqHxDp4dq/degISTiygVodwDmsIQ/4l+ZexAoGBAK4/Ffv9hM2ygDgUKKwq\n7hMJ4LGfEf4CeqWplaYKTt/HtfHkfA2/s/rXz9tWyblbOBRVCJgZ0heurWPwXxN2\n9cdlNqU2ivrdfllY6eEp/pIvJnkTFIVkbJ72b+R6Xn3reF+A+d5PdUgRpnG6+Ye+\nVvyYI4aj32Fdwc0W8JKrOhXz\n-----END PRIVATE KEY-----\n",
                "client_email": "mail-engine-411414@appspot.gserviceaccount.com",
                "client_id": "116316020823060397699",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/mail-engine-411414%40appspot.gserviceaccount.com",
                "universe_domain": "googleapis.com",
            }

            self._storage_service_account_project_id = (
                self._storage_service_account_info["project_id"]
            )

            self._storage_credentials = (
                service_account.Credentials.from_service_account_info(
                    info=self._storage_service_account_info
                )
            )

            self._storage_client = storage.Client(credentials=self._storage_credentials)

            return self._storage_client
        except Exception as e:
            print(f"Exception {e} while creating cloud storage client")
            traceback.print_exc()

            return None

    def upload_file_to_cloud_storage_bucket(self, file_name, file_path):
        try:
            self.logger.debug(
                f"Attempting to upload {file_name} existing at {file_path} to cloud storage"
            )
            client = self.create_cloud_storage_client()
            if client is None:
                raise Exception(f"Cannot create cloud storage client")

            # storage client is created.
            # attempt upload.
            bucket_name = "olmstead-property-letters"

            destination_directory = "Export_For_Mail_House"

            new_file_name = str(file_name).strip("./").strip("/tmp/")

            self.logger.debug("Attempt to create blob")

            self.logger.debug(f"storage location = {destination_directory}/{file_name}")

            # create a blob for the file.
            blob = self._storage_client.bucket(bucket_name=bucket_name).blob(
                f"{destination_directory}/{new_file_name}"
            )

            self.logger.debug(f"Attempt to upload file at {file_path}")

            # upload the blob using the file
            blob.upload_from_filename(f"{file_path}")

            self.logger.debug(
                f"{file_name} existing at {file_path} uploaded to bucket {bucket_name} at destination {destination_directory}"
            )
            return f"{bucket_name}/{destination_directory}/{new_file_name}"

        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Could not upload file {file_name} to cloud storage")

            return None

    def create_signed_url(
        self,
        file_name,
        destination_directory="Export_For_Mail_House",
        bucket_name="olmstead-property-letters",
    ):
        try:
            # Construct the full path to the file (including directory if provided)
            file_path = (
                f"{destination_directory}/{file_name}"
                if destination_directory
                else file_name
            )

            self.logger.debug(
                f"Attempting to create signed url for {file_name} existing at {file_path} to cloud storage"
            )
            if hasattr(self, "_storage_client") and self._storage_client is None:
                self.create_cloud_storage_client()
            else:
                self.create_cloud_storage_client()

            # Get a reference to the blob (file)
            blob = self._storage_client.bucket(bucket_name).blob(file_path)

            # Generate a v4 signed URL for downloading the blob with a specific expiration time (24 hours)
            url = blob.generate_signed_url(
                version="v4", method="GET", expiration=datetime.timedelta(minutes=1440)
            )

            return url

        except Exception as e:
            self.logger.error(f"Exception {e} in generating signed URL for {file_name}")
            return None

    def upload_log_to_cloud_storage_bucket(self, file_name=None, file_path=None):
        try:
            if file_name is None:
                file_name = self.log_file_name

            self.logger.debug(
                f"Attempting to upload {file_name} existing at {file_path} to cloud storage"
            )
            if hasattr(self, "_storage_client") and self._storage_client is None:
                self.create_cloud_storage_client()
            else:
                self.create_cloud_storage_client()
            # storage client is created.
            # attempt upload.
            bucket_name = "olmstead-property-letters"

            destination_directory = "Export_For_Mail_House/logs"

            self.logger.debug("Attempt to create blob")

            self.logger.debug(f"storage location = {destination_directory}/{file_name}")

            # create a blob for the file.
            blob = self._storage_client.bucket(bucket_name=bucket_name).blob(
                f"{destination_directory}/{file_name.strip('./')}"
            )

            self.logger.debug(f"Attempt to upload file at {file_path}")

            # upload the blob using the file
            blob.upload_from_filename(f"{file_path}")

            self.logger.debug(
                f"{file_name} existing at {file_path} uploaded to bucket {bucket_name} at destination {destination_directory}"
            )
            Path(file_path).unlink()
            return f"{bucket_name}/{destination_directory}/{file_name}"

        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Could not upload file {file_name} to cloud storage")

            return None


class UploadFileToCloudStorage:
    def __init__(self, filename, file_parent_directory) -> None:
        self._filename = filename
        self._file_parent_directory = file_parent_directory

    def create_cloud_storage_client(self):
        """
        Creates a Google Cloud Storage client for handling file uploads and downloads.
        """

        self._storage_service_account_info = {
            "type": "service_account",
            "project_id": "mail-engine-411414",
            "private_key_id": "a7ad4ee8f14e0776b76aa73a94e7bc584e363b1c",
            "private_key": "-----BEGIN PRIVATE KEY-----MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDOFQ58wLHF9HveqRgPjy2+3aHExea5E5m3pczHpdx/FbDZE/CTI80DYy1eXejH2WPCOAfYh8JWqiB0429KGJm17hBGjgrU40VxwGbKFiEUzZFwDDSoOyMNzmr1umMcZoVYZu5fhogmksoUeaYgzwYLbtUtN5MVhG5bjG3odaYasl1VCxbnq4vlnDr6v1iaPCUZlwyP4ZpPBTdUrMrLyLDE4hym4kXjrRtMa5ziALqmDtIiFPqb56QJv/Y+AqOfLpOmZbreq6NmYmoORq+dUCr2VsPIUwVlUlEj9mdEbFds/eLSjJBgspg1OzEDLr2knJIypEl8yxey6O7OqIaHSABTAgMBAAECggEAGJqx+poE/4Xjfh+BJGZrR0TtyfD4zXL2INN2QgXe7/tWfGxhhie5k+R057Xdk3K0ct0+ro7y2GcCbgIzaMzMGUj9f3b4+xbRVTXr78e5lmK+sPp8FPPOiRjyczkYGS1yUS4k2pnUyoou/0fQ2kztQ1Mtt3LRqDmB9YMsIhtdxAT9vj8fpWUInG/x2PQsSUG3BCNmdud1TP6KiLJBorRcsPkFY7UdOHO1FDKZVoMCAgdjhU/VsePP31evriFhsbQSHIaopKpj46Z3mMLnL0YC8dzZE6JFP7ubB5UrfmHQ6C0otxb6SK5Qc7oSwleVtEkg7PyvAKwZ3XrfBkIY1atLvQKBgQDpXXkgEZ5+iGi0wZ/r1aZ0ELeYAo7Iw/ZJ5HwATup3VQx0gYMiv2Vymnf/BPLxp360yrl81AqUBx9aO868ZDFD/CwFVR24fxg88fUzmn99MLPOYFcPm+V/dlrYGRfHA1heKleO6ifZ0RxqeLjmWR3dQFjBcRGPSzpOeTGpdo0+lQKBgQDiEiQu7hvn5zgg0VuVv8qi45f/EatYllWx7unMiK3jJjJE1xP0SamMqLxa74tfHloDoof4i/UFC3WRh1AYZwTXowCYQrxXn4tf5b6/Qa0GknTYTenF2u8PT+4i54/Odk5gxh8B1YjkDEGBgln2xgtznyfaYn0ieGB2pa8q8PTRRwKBgQC1Nbo6prPWOYJk6f2Omca5VcnaphHN4C09T8jiTGVQ6J/VUEROFSVnin8nbeZgs9l19f126wiTzbwQy2RLcdm3mvdr5J5Bh5+Ao7ntqkjZI5pb5P91xW6+PV9pcoK+LODPrj1zYmwzfWd9XeTmKclA5xiEjZI/HQFXslBX8RDYyQKBgHoc4n6XD2vMcX0ImTfiSUqDNW8J1EwdqFxceN2KVqMD1tNoedbSk0rvOg7EpbvTYTvlZxzSPJ9k+TSKqrEDtJHl8kRMh/+splQ+fTJB/3w1T/gm3ceJ4ueafT9NZVhYfy221rje5vqHxDp4dq/degISTiygVodwDmsIQ/4l+ZexAoGBAK4/Ffv9hM2ygDgUKKwq\n7hMJ4LGfEf4CeqWplaYKTt/HtfHkfA2/s/rXz9tWyblbOBRVCJgZ0heurWPwXxN2\n9cdlNqU2ivrdfllY6eEp/pIvJnkTFIVkbJ72b+R6Xn3reF+A+d5PdUgRpnG6+Ye+\nVvyYI4aj32Fdwc0W8JKrOhXz\n-----END PRIVATE KEY-----\n",
            "client_email": "mail-engine-411414@appspot.gserviceaccount.com",
            "client_id": "116316020823060397699",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/mail-engine-411414%40appspot.gserviceaccount.com",
            "universe_domain": "googleapis.com",
        }

        if self._storage_service_account_info is not None:
            self._storage_service_account_project_id = (
                self._storage_service_account_info["project_id"]
            )

            self._storage_credentials = (
                service_account.Credentials.from_service_account_info(
                    info=self._storage_service_account_info
                )
            )

            self._storage_client = storage.Client(credentials=self._storage_credentials)

    def upload_file_to_cloud_storage_bucket(self, file_name, file_path):
        try:
            self.logger.debug(
                f"Attempting to upload {file_name} existing at {file_path} to cloud storage"
            )
            if hasattr(self, "_storage_client") and self._storage_client is None:
                self.create_cloud_storage_client()
            else:
                self.create_cloud_storage_client()
            # storage client is created.
            # attempt upload.
            bucket_name = "olmstead-property-letters"

            destination_directory = "Received_From_Mail_House"

            self.logger.debug("Attempt to create blob")

            self.logger.debug(f"storage location = {destination_directory}/{file_name}")

            # create a blob for the file.
            blob = self._storage_client.bucket(bucket_name=bucket_name).blob(
                f"{destination_directory}/{file_name.strip('./')}"
            )

            self.logger.debug(f"Attempt to upload file at {file_path}")

            # upload the blob using the file
            blob.upload_from_filename(f"{file_path}")

            self.logger.debug(
                f"{file_name} existing at {file_path} uploaded to bucket {bucket_name} at destination {destination_directory}"
            )
            return f"{bucket_name}/{destination_directory}/{file_name}"

        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Could not upload file {file_name} to cloud storage")

            return None


if __name__ == "__main__":
    data_frame = pd.read_csv(
        "./services/raw.csv",
        dtype={
            "APN": "string",
            "Mail_Zip_Code": "string",
            "Property_Zip_Code": "string",
            "Mail_Zip_Code5_R": "string",
            "Mail_Zip_Code5_N": "string",
        },
    )
    data_frame = data_frame[
        (data_frame["Property_State_Name"].str.lower() == "georgia")
    ]
    print(data_frame.head())
    # exit(0)

    b = PropertyRecordsPreProcessor(
        dataframe=data_frame,
    )

    print(b.is_valid_dataframe)

    results = b.pre_process_fetched_results()

    # drive_reader = GoogleDriveFileReader()
    # drive_reader.download_pricing_research_file_locally()
    print(results)
