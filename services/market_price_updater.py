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
import os

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
            #self.logger = logging.getLogger("GoogleDriveFileReader")
            #self.logger.setLevel(logging.DEBUG)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            file_handler = logging.FileHandler(self.log_file_name)
            file_handler.setFormatter(formatter)
            #self.logger.addHandler(file_handler)

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

            #self.logger.debug("creating service account credentials")

            self._credentials = service_account.Credentials.from_service_account_info(
                service_account_info_json
            )

            #self.logger.debug("creating drive service")
            self._service = build("sheets", "v4", credentials=self._credentials)
            #self.logger.debug(type(self._service))

        except Exception as e:
            #self.logger.error(
            #     f"Exception {str(e)} while creating drive service account"
            # # )
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
                #self.logger.debug("Download %d%%." % int(status.progress() * 100))

        except Exception as e:
            #self.logger.error(f"Exception {str(e)}")
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
            #self.logger = logging.getLogger("PropertyRecordsPreProcessor")
            #self.logger.setLevel(logging.DEBUG)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            file_handler = logging.FileHandler(self.log_file_name)
            file_handler.setFormatter(formatter)
            #self.logger.addHandler(file_handler)
            self._dataframe = dataframe


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
                #self.logger.error("cannot load pricing research")
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


    

    def process_batch(self, df_batch):
        print("processing batch")
        #self.logger.debug("Processing batch.")
        return df_batch.apply(self.find_price_per_acre, axis=1)

    def pre_process_fetched_results(self):
        try:
            if self._dataframe is None:
                return {"message": "failed", "error": "DataFrame is invalid"}
            # self._dataframe  = self._dataframe[self._dataframe["Property_State_Name"].str.lower() == "georgia"]

            print(len(self._dataframe))
            self._dataframe["Price_per_Acre"] = np.nan
            self._dataframe["Market_Price"] = 0

            if not self._pricing_research_for_state:
                return {"message": "failed", "error": "Pricing research data is invalid"}

            batch_size = self._dataframe.shape[0] // 7
            batches = [self._dataframe.iloc[i:i + batch_size] for i in range(0, self._dataframe.shape[0], batch_size)]
            result_batches = []

            with ProcessPoolExecutor() as executor:
                futures = [executor.submit(self.process_batch, batch) for batch in batches]
                result_batches = [future.result() for future in futures]

            self._dataframe = pd.concat(result_batches)
            self._dataframe.to_csv('/home/itechnolabs/Desktop/Processed_GA_Properties.csv', index=False)
        except Exception as e:
            #self.logger.error(f"Exception in pre_process_fetched_results: {str(e)}")
            traceback.print_exc()

    def _find_matching_row(
        self, pricing_data, state, county, zip_code, apn, lot_acreage, **kwargs
    ):
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
                #self.logger.debug(
                #     f"No match found for {state}, {county}, {zip_code}, {apn}, {lot_acreage}"
                # )
                return None

            # Sort by DataFrame index if no specific criterion is available
            matching_rows = matching_rows.sort_index(ascending=False)

            # Select the row with the highest index
            best_match = matching_rows.iloc[0]

            return best_match

        except Exception as find_matching_rows_exc:
            #self.logger.warning(
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
            
            #self.logger.debug(f"Processing row: {row.name}")
            # #self.logger.debug(str(row["Property_State_Name_Short"]).strip().lower() if )
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

            #self.logger.debug(f"{row['Property_State_Name']} {sheet_name}")

            pricing_data = self._pricing_research_for_state[sheet_name]

            # Extract pricing data for the state from Excel file
            #self.logger.debug("extract pricing data")

            # Check if pricing data is valid
            if (
                pricing_data is None
                or not isinstance(pricing_data, pd.DataFrame)
                or (
                    isinstance(pricing_data, pd.DataFrame)
                    and pricing_data.shape[0] <= 0
                )
            ):
                #self.logger.debug("pricing data invalid")
                # #self.logger.debug(type(pricing_data))
                # #self.logger.debug(pricing_data)
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

            #self.logger.debug("--- matched row ----")

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
                #self.logger.debug("----matched_row is valid----")

                row["Price_per_Acre"] = round(
                    matched_row["Per Acre Pricing - Value"], 2
                )
            else:
                #self.logger.debug(
                #     f"the calculation of pricing per acre from pricing research csv was inconclusive {state} {county} {apn} {zip_code} {lot_acreage} {type(matched_row)}"
                # )
                # #self.logger.debug(type(matched_row))
                # if isinstance(matched_row, pd.Series):
                #     #self.logger.debug("Per Acre Pricing - Value" in matched_row.index)
                #     #self.logger.debug(not pd.isna(matched_row["Per Acre Pricing - Value"]))
                #     #self.logger.debug(type(matched_row["Per Acre Pricing - Value"]))

                writeable = (
                    f"{row['Owner_ID']},{state},{county},{zip_code},{apn},{lot_acreage}"
                )

                with open(self.missed_properties_csv, "+a") as f:
                    f.write(writeable + "\n")
                    f.close()

                # exit(0)

                # #self.logger.debug(type(matched_row))
                row["Price_per_Acre"] = 0.00

            # Update Market Price
            row = self.update_market_price(row)

            
            return row

        except Exception as find_price_per_acre_exception:
            #self.logger.error(
            #     f"Exception while calculating price per acre {str(find_price_per_acre_exception)}"
            # )
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
            #self.logger.error(
            #     f"Exception while updating market price {str(update_market_price_exception)}"
            # )
            traceback.print_exc()

        finally:
            return row

def process_georgia_batches(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)

    for file_name in os.listdir(input_folder):
        if file_name.endswith(".csv"):
            file_path = os.path.join(input_folder, file_name)

            # Load the DataFrame with Property_Zip_Code as string
            df = pd.read_csv(file_path, dtype={
            "APN": str,
            "Property_State_Name": str,
            "Property_County_Name": str,
            "Lot_Acreage": pd.Float64Dtype(),
            "Lot_Area": pd.Float64Dtype(),
            "Property_Zip_Code": str,
            "Property_Zip_Code_STD": str,
            "Property_Address": str,
            "Property_Address_Full": str,
            "Property_City": str,
            "Property_County_FIPS": str,
            "Property_Range": str,
            "Property_Section": str,
            "Property_Subdivision": str,
            "Property_Township": str,
            "Property_Zoning": str,
            "Property_Latitude": pd.Float64Dtype(),
            "Property_Longitude": pd.Float64Dtype(),
            "Significant_Flood_Zones": pd.Float64Dtype(),
            "Unformatted_APN": str,
            "Alternate_APN": str,
            "Wetlands_Total": pd.Float64Dtype(),
            "Year_Built": pd.Float64Dtype(),
            "slope_mean": pd.Float64Dtype(),
            "Owner_ID": str,
            "Plot_ID": str,
            "SITE_AAF": str,
            "SITE_CCD": str,
            "SITE_ZIP": pd.Int64Dtype(),
            "OWNER_9B": str,
            "MAIL_A9D": str,
            "DATE_TE2": str,
            "VAL_TRA7": pd.Float64Dtype(),
            "ACREAGE": pd.Float64Dtype(),
            "AGGR_A68": pd.Float64Dtype(),
            "AGGR_GD": str,
            "AGGR_L57": pd.Int64Dtype(),
            "LAND_SBC": pd.Float64Dtype(),
            "UNITS_BB": pd.Float64Dtype(),
            "YR_BLT": str,
            "ZONING": str,
            "USE_COBE": str,
            "USE_CO2F": str,
            "STATEFP10": pd.Int64Dtype(),
            "COUNTYFP10": pd.Int64Dtype(),
            "GEOID10": pd.Int64Dtype(),
            "NAME10": str,
            "NAMELSAD10": str,
            "totpop10": pd.Int64Dtype(),
            "WFD": str,
            "RDC_AAA": str,
            "MNGWPD": str,
            "MPO": str,
            "MSA": str,
            "F1HR_NA": str,
            "F8HR_NA": str,
            "Reg_Comm": str,
            "Acres": pd.Float64Dtype(),
            "Sq_Miles": pd.Float64Dtype(),
            "Label": str,
            "GlobalID": str,
            "last_edite": str,
            "Clay": pd.Float64Dtype(),
            "Clay_loam": pd.Float64Dtype(),
            "Coarse_sand": pd.Float64Dtype(),
            "Coarse_sandy_loam": pd.Float64Dtype(),
            "Fine_sand": pd.Float64Dtype(),
            "Fine_sandy_loam": pd.Float64Dtype(),
            "Loam": pd.Float64Dtype(),
            "Loamy_coarse_sand": pd.Float64Dtype(),
            "Loamy_fine_sand": pd.Float64Dtype(),
            "Loamy_sand": pd.Float64Dtype(),
            "Sand": pd.Float64Dtype(),
            "Sandy_clay": pd.Float64Dtype(),
            "Sandy_clay_loam": pd.Float64Dtype(),
            "Sandy_loam": pd.Float64Dtype(),
            "Silt_loam": pd.Float64Dtype(),
            "Silty_clay": pd.Float64Dtype(),
            "Silty_clay_loam": pd.Float64Dtype(),
            "Very_fine_sandy_loam": pd.Float64Dtype(),
            "A": pd.Float64Dtype(),
            "AE": pd.Float64Dtype(),
            "AH": pd.Float64Dtype(),
            "AREA_NOT_INCLUDED": pd.Float64Dtype(),
            "D": pd.Float64Dtype(),
            "VE": pd.Float64Dtype(),
            "X": pd.Float64Dtype(),
            "slope_min": pd.Float64Dtype(),
            "slope_max": pd.Float64Dtype(),
            "slope_std": pd.Float64Dtype(),
            "num_buildings": pd.Int64Dtype(),
            "largest_building": pd.Float64Dtype(),
            "smallest_building": pd.Float64Dtype(),
            "total_building_area": pd.Float64Dtype(),
            "nearest_road_type": str,
            "distance_to_nearest_road_from_centroid": pd.Float64Dtype(),
            "road_length": pd.Float64Dtype(),
            "trees_percentage": pd.Float64Dtype(),
            "built_percentage": pd.Float64Dtype(),
            "grass_percentage": pd.Float64Dtype(),
            "crops_percentage": pd.Float64Dtype(),
            "shrub_and_scrub_percentage": pd.Float64Dtype(),
            "bare_percentage": pd.Float64Dtype(),
            "water_percentage": pd.Float64Dtype(),
            "flooded_vegetation_percentage": pd.Float64Dtype(),
            "snow_and_ice_percentage": pd.Float64Dtype(),
            "Estuarine_and_Marine_Deepwater": pd.Float64Dtype(),
            "Estuarine_and_Marine_Wetland": pd.Float64Dtype(),
            "Freshwater_Emergent_Wetland": pd.Float64Dtype(),
            "Freshwater_Forested_Shrub_Wetland": pd.Float64Dtype(),
            "Freshwater_Pond": pd.Float64Dtype(),
            "Lake": pd.Float64Dtype(),
            "Riverine": pd.Float64Dtype(),
            "parcel_area": pd.Float64Dtype(),
            "plot_area_1": pd.Float64Dtype(),
            "largest_rect_area": pd.Float64Dtype(),
            "percent_rectangle": pd.Float64Dtype(),
            "largest_square_area": pd.Float64Dtype(),
            "percent_square": pd.Float64Dtype(),
            "largest_rect_area_cleaned": pd.Float64Dtype(),
            "largest_square_area_cleaned": pd.Float64Dtype(),
            "Market_Price": pd.Float64Dtype(),})
            



            processor = PropertyRecordsPreProcessor(dataframe=df)
            processor.pre_process_fetched_results()

            # Filter for Georgia data and save to output folder
            georgia_df = processor._dataframe[processor._dataframe["Property_State_Name"].str.lower() == "georgia"]
            if not georgia_df.empty:
                output_file_path = os.path.join(output_folder, f"processed_georgia_{file_name}")
                georgia_df.to_csv(output_file_path, index=False)
                print(f"Saved processed Georgia data to {output_file_path}")

            # Delete the file from the input folder
            os.remove(file_path)
            print(f"Deleted {file_path} from input folder.")


def process_nc_batches(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)

    for file_name in os.listdir(input_folder):
        if file_name.endswith(".csv"):
            file_path = os.path.join(input_folder, file_name)

            # Load the DataFrame with Property_Zip_Code as string
            df = pd.read_csv(file_path, dtype={
            "APN": str,
            "Property_State_Name": str,
            "Property_County_Name": str,
            "Lot_Acreage": pd.Float64Dtype(),
            "Lot_Area": pd.Float64Dtype(),
            "Property_Zip_Code": str,
            "Property_Zip_Code_STD": str,
            "Property_Address": str,
            "Property_Address_Full": str,
            "Property_City": str,
            "Property_County_FIPS": str,
            "Property_Range": str,
            "Property_Section": str,
            "Property_Subdivision": str,
            "Property_Township": str,
            "Property_Zoning": str,
            "Property_Latitude": pd.Float64Dtype(),
            "Property_Longitude": pd.Float64Dtype(),
            "Significant_Flood_Zones": pd.Float64Dtype(),
            "Unformatted_APN": str,
            "Alternate_APN": str,
            "Wetlands_Total": pd.Float64Dtype(),
            "Year_Built": pd.Float64Dtype(),
            "slope_mean": pd.Float64Dtype(),
            "Owner_ID": str,
            "Plot_ID": str,
            "SITE_AAF": str,
            "SITE_CCD": str,
            "SITE_ZIP": pd.Int64Dtype(),
            "OWNER_9B": str,
            "MAIL_A9D": str,
            "DATE_TE2": str,
            "VAL_TRA7": pd.Float64Dtype(),
            "ACREAGE": pd.Float64Dtype(),
            "AGGR_A68": pd.Float64Dtype(),
            "AGGR_GD": str,
            "AGGR_L57": pd.Int64Dtype(),
            "LAND_SBC": pd.Float64Dtype(),
            "UNITS_BB": pd.Float64Dtype(),
            "YR_BLT": str,
            "ZONING": str,
            "USE_COBE": str,
            "USE_CO2F": str,
            "STATEFP10": pd.Int64Dtype(),
            "COUNTYFP10": pd.Int64Dtype(),
            "GEOID10": pd.Int64Dtype(),
            "NAME10": str,
            "NAMELSAD10": str,
            "totpop10": pd.Int64Dtype(),
            "WFD": str,
            "RDC_AAA": str,
            "MNGWPD": str,
            "MPO": str,
            "MSA": str,
            "F1HR_NA": str,
            "F8HR_NA": str,
            "Reg_Comm": str,
            "Acres": pd.Float64Dtype(),
            "Sq_Miles": pd.Float64Dtype(),
            "Label": str,
            "GlobalID": str,
            "last_edite": str,
            "Clay": pd.Float64Dtype(),
            "Clay_loam": pd.Float64Dtype(),
            "Coarse_sand": pd.Float64Dtype(),
            "Coarse_sandy_loam": pd.Float64Dtype(),
            "Fine_sand": pd.Float64Dtype(),
            "Fine_sandy_loam": pd.Float64Dtype(),
            "Loam": pd.Float64Dtype(),
            "Loamy_coarse_sand": pd.Float64Dtype(),
            "Loamy_fine_sand": pd.Float64Dtype(),
            "Loamy_sand": pd.Float64Dtype(),
            "Sand": pd.Float64Dtype(),
            "Sandy_clay": pd.Float64Dtype(),
            "Sandy_clay_loam": pd.Float64Dtype(),
            "Sandy_loam": pd.Float64Dtype(),
            "Silt_loam": pd.Float64Dtype(),
            "Silty_clay": pd.Float64Dtype(),
            "Silty_clay_loam": pd.Float64Dtype(),
            "Very_fine_sandy_loam": pd.Float64Dtype(),
            "A": pd.Float64Dtype(),
            "AE": pd.Float64Dtype(),
            "AH": pd.Float64Dtype(),
            "AREA_NOT_INCLUDED": pd.Float64Dtype(),
            "D": pd.Float64Dtype(),
            "VE": pd.Float64Dtype(),
            "X": pd.Float64Dtype(),
            "slope_min": pd.Float64Dtype(),
            "slope_max": pd.Float64Dtype(),
            "slope_std": pd.Float64Dtype(),
            "num_buildings": pd.Int64Dtype(),
            "largest_building": pd.Float64Dtype(),
            "smallest_building": pd.Float64Dtype(),
            "total_building_area": pd.Float64Dtype(),
            "nearest_road_type": str,
            "distance_to_nearest_road_from_centroid": pd.Float64Dtype(),
            "road_length": pd.Float64Dtype(),
            "trees_percentage": pd.Float64Dtype(),
            "built_percentage": pd.Float64Dtype(),
            "grass_percentage": pd.Float64Dtype(),
            "crops_percentage": pd.Float64Dtype(),
            "shrub_and_scrub_percentage": pd.Float64Dtype(),
            "bare_percentage": pd.Float64Dtype(),
            "water_percentage": pd.Float64Dtype(),
            "flooded_vegetation_percentage": pd.Float64Dtype(),
            "snow_and_ice_percentage": pd.Float64Dtype(),
            "Estuarine_and_Marine_Deepwater": pd.Float64Dtype(),
            "Estuarine_and_Marine_Wetland": pd.Float64Dtype(),
            "Freshwater_Emergent_Wetland": pd.Float64Dtype(),
            "Freshwater_Forested_Shrub_Wetland": pd.Float64Dtype(),
            "Freshwater_Pond": pd.Float64Dtype(),
            "Lake": pd.Float64Dtype(),
            "Riverine": pd.Float64Dtype(),
            "parcel_area": pd.Float64Dtype(),
            "plot_area_1": pd.Float64Dtype(),
            "largest_rect_area": pd.Float64Dtype(),
            "percent_rectangle": pd.Float64Dtype(),
            "largest_square_area": pd.Float64Dtype(),
            "percent_square": pd.Float64Dtype(),
            "largest_rect_area_cleaned": pd.Float64Dtype(),
            "largest_square_area_cleaned": pd.Float64Dtype(),
            "Market_Price": pd.Float64Dtype(),})
            



            processor = PropertyRecordsPreProcessor(dataframe=df)
            processor.pre_process_fetched_results()

            # Filter for Georgia data and save to output folder
            georgia_df = processor._dataframe[processor._dataframe["Property_State_Name"].str.lower() == "north carolina"]
            if not georgia_df.empty:
                output_file_path = os.path.join(output_folder, f"processed_nc_{file_name}")
                georgia_df.to_csv(output_file_path, index=False)
                print(f"Saved processed NC data to {output_file_path}")

            # Delete the file from the input folder
            os.remove(file_path)
            print(f"Deleted {file_path} from input folder.")
if __name__ == "__main__":
    # input_folder = '/home/itechnolabs/Desktop/Georgia_Batches'  # Change this path to your input folder
    # output_folder = '/home/itechnolabs/Desktop/Georgia_Processed_Outputs'  # Change this path to your output folder
    # process_georgia_batches(input_folder,output_folder)
    input_folder = '/home/itechnolabs/Desktop/North_Carolina_Batches'  # Change this path to your input folder
    output_folder = '/home/itechnolabs/Desktop/North_Carolina_Processed_Outputs'  # Change this path to your output folder
    process_nc_batches(input_folder, output_folder)
