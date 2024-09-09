import datetime
import io
import logging
import traceback
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError
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
                f"Log-GoogleDriveFileReader-{self.processing_start_date}.log"
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
                "private_key_id": "1146f1910f69c66001da6667bb4cea3128457576",
                "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDKDv07A7OU6kL3\n0F7Ev6JH3xh9KzAhvIvYINRURCUsNU1G3rjO5V76Rp59vztNUrutfWm+W5KUZpXc\nSdfK8RzQ5FN+bhXJL2K951Aaw1DFSNaKXRiafYHhkqzLipMk/WIb8rVCR3w1SWhU\nEU8WrZa9VyJ+VJShXpN6klSuX/Ky5axLrqS97A8zEQqiYyxzK/1P79ol4jBBS3nX\n36hDimUMac50hWoJVNATRjx9512853nbtMN0/eI0slwTJ4UToUL8QiISXhqNBIH5\nodQ34I1cVtLUEtT7lDbsfshiYcQgMudq5KBON4NGxPx5/YTDbskbmN/kKt/Oi55A\nmCdPsnXNAgMBAAECggEAGy+qmzDbjfb6BTFEOvkkusJV32t66aMedrkuEFtt2/Wc\n/OLVH0NFxOtRAk7DShtOVKCp5FqaTXzC/5Vw4yWPULzTzhA+PqgxQydXBA7Q9IZh\nFVNCzvCBk+nh9W3i7+hmtadxg2YuXFYQ4zTD/SNkg3uwkKpNKCwbMDP1kFOpCjA3\nAPhDA5sE9V876xSKFLEtFbWqAV4HPNB56kwJso/OnTSpmDmArt06hsxznpNpDC4x\nRXaL3mgTIAOxP/3j3P9YO3UA05HOpnDIy2VpmKGhBm/fn5MWZ9JPHPeVS+5sfyCT\n3XD/A8v4ig0oN/eHzqhzrQ2oAuzR/NOz/I8hdMnH+QKBgQDr41cgix+qsvXoSWWn\nZj1c6Z0Xkm+i1lyA1Gfn1uZ60aVnoibLcNBFNn6VYBWWf0MLiKu75/YBG/O0vRGz\n/0QUliUIz2xtpDp6TN5/77zsYIb0MmdnrMiy7NcQAa6NvD74jbiGjOzXwQp1iqKJ\n1bJH1nA9G4n/soHSOqiVNogZCQKBgQDbSUMktwLA+LrVK6Z4A/m3VNEaRGuh5bg3\n4eaGVcM+NmeBAUS/QtL2RE14+AECpUYInpjlnQsz26ih2APzlndnwg899M6N7o+u\nwg86LjE2IM81GT9tXlXoT9nwQ7vfmqA5y7/n006BzV/wNb6NncumcExlA9hDsYcO\nISZ2kht7pQKBgGUQp3DDCtNJD8DxSYN6c4oOcYb676e17jYoS0hys5cloZeQSszZ\nTfKJkZyQaU9swR389Y9xp937yuPCKgaTtOiZF875h/xF4+QocSAhaDFifg+8VL9/\nsRGwFci+37nMULKPjeLgGE2sYL3RoygpDdRnlPkphmuuYJ7hEP+1OIR5AoGAN4v6\nVx2ItAsNgJwbAgG3ysnQYf/857jCDl3JwDOPTn4Hf/UrTeuGdt1cZo0j7GCjOeG6\nx0cdpFg7Aiwu7BAsVPsiU0Zk4C0S9miv5MP4sUZkVoX7vB+OUVeQ1DzEJWMMvTcw\neG/dTiIQ8E8c4tCa2qFNqLTtnlAk7t5U2OovzgUCgYAQ3hhmCJnkTLGVjDq8CTbz\nqmLGJ/gKFcwrj1I3JdGswwrcGiOqkdPCa3Yl/WTNBHfNuIOai1tVvRzNkOZnpxmq\nFVmyHbIQg4SpI0TT8VP+dl6Vl+/fvCx6IJAAMMqWchGq0JoFuqLBhPmNFI1G25m+\nVvo64pX2d5Ud6PxJNqkqEQ==\n-----END PRIVATE KEY-----\n",
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
        "Property_State_Name",
        "Property_State_Name_Short",
        "Property_County_Name",
        "Property_Zip_Code",
        "Property_City",
        "Property_Address_Full",
        "Owner_First_Name",
        "Owner_Last_Name",
        "Owner_Full_Name",
        "Owner_2_First_Name",
        "Owner_2_Last_Name",
        "Owner_Type",
        "APN",
        "Lot_Acreage",
        "Market_Price",
        "Date",
        "Mail_Address_Full",
        "Owner_Mailing_Name",
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
                f"missed_properties-{self.processing_start_date}.csv"
            )
            writeable = "Owner_ID,State,County,Zip Code,APN,Lot Acreage\n"

            with open(self.missed_properties_csv, "+a") as f:
                f.write(writeable)
                f.close()

            self.log_file_name = (
                f"Log-PropertyRecordsPreProcessor-{self.processing_start_date}.log"
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
                "Owner_First_Name",
                "Owner_Last_Name",
                "Owner_Full_Name",
                # "Owner_Mailing_Name",
                "Property_City",
            ]

            self._upper_case_columns = [
                "APN",
                # "Property_State_Name_Short",
                "Owner_ID",
            ]

            # self._dataframe[self._title_case_columns] = self._dataframe[
            #     self._title_case_columns
            # ].applymap(lambda x: str(x).title() if pd.notna(x) else "")

            # self._dataframe[self._upper_case_columns] = self._dataframe[
            #     self._upper_case_columns
            # ].applymap(lambda x: str(x).upper() if pd.notna(x) else "")

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
                    sheet_name=sheet_name
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
        try:
            if (
                pricing_data is None
                or not isinstance(pricing_data, pd.DataFrame)
                or pricing_data.shape[0] == 0
            ):
                return None

            self.logger.debug(f"passed {state}, {county}, {zip_code}, {lot_acreage}")

            pricing_data.sort_values(by=["Per Acre Pricing - Value"])

            state_null = pricing_data["State"].isnull()
            county_null = pricing_data["County"].isnull()
            zip_code_null = pricing_data["Zip Code"].isnull()
            acreage_null = (
                pricing_data["Starting Acreage"].isnull()
                & pricing_data["Ending Acreage"].isnull()
            )
            apn_section_null = pricing_data["APN Section"].isnull()

            state_match = pricing_data["State"].str.lower() == str(state).lower()
            county_match = pricing_data["County"].str.lower() == str(county).lower()
            zip_code_match = pricing_data["Zip Code"] == zip_code
            acreage_match = (
                pricing_data["Starting Acreage"].fillna(0) <= lot_acreage
            ) & (pricing_data["Ending lot_acreage"].fillna(0) >= lot_acreage)
            apn_section_match = pricing_data["APN Section"].apply(
                lambda x: str(apn).lower().startswith(str(x).lower().strip())
            )

            results = None
            reason = None

            if not pricing_data[
                state_match
                & county_match
                & zip_code_match
                & apn_section_match
                & acreage_match
            ].empty:
                results = pricing_data[
                    state_match
                    & county_match
                    & zip_code_match
                    & apn_section_match
                    & acreage_match
                ]
                reason = "pricing_data[state_match & county_match & zip_code_match & apn_section_match & acreage_match]"

            elif not pricing_data[
                state_match
                & county_match
                & zip_code_match
                & apn_section_match
                & acreage_null
            ].empty:
                results = pricing_data[
                    state_match
                    & county_match
                    & zip_code_match
                    & apn_section_match
                    & acreage_null
                ]
                reason = "pricing_data[state_match & county_match & zip_code_match & apn_section_match & acreage_null]"

            elif not pricing_data[
                state_match
                & county_match
                & zip_code_match
                & apn_section_null
                & acreage_match
            ].empty:
                results = pricing_data[
                    state_match
                    & county_match
                    & zip_code_match
                    & apn_section_null
                    & acreage_match
                ]
                reason = "pricing_data[state_match & county_match & zip_code_match & apn_section_null & acreage_match]"

            elif not pricing_data[
                state_match
                & county_match
                & zip_code_match
                & apn_section_null
                & acreage_null
            ].empty:
                results = pricing_data[
                    state_match
                    & county_match
                    & zip_code_match
                    & apn_section_null
                    & acreage_null
                ]
                reason = "pricing_data[state_match & county_match & zip_code_match & apn_section_null & acreage_null]"

            elif not pricing_data[
                state_match
                & county_match
                & zip_code_null
                & apn_section_match
                & acreage_match
            ].empty:
                results = pricing_data[
                    state_match
                    & county_match
                    & zip_code_null
                    & apn_section_match
                    & acreage_match
                ]
                reason = "pricing_data[state_match & county_match & zip_code_null & apn_section_match & acreage_match]"

            elif not pricing_data[
                state_match
                & county_match
                & zip_code_null
                & apn_section_match
                & acreage_null
            ].empty:
                results = pricing_data[
                    state_match
                    & county_match
                    & zip_code_null
                    & apn_section_match
                    & acreage_null
                ]
                reason = "pricing_data[state_match & county_match & zip_code_null & apn_section_match & acreage_null]"

            elif not pricing_data[
                state_match
                & county_match
                & zip_code_null
                & apn_section_null
                & acreage_match
            ].empty:
                results = pricing_data[
                    state_match
                    & county_match
                    & zip_code_null
                    & apn_section_null
                    & acreage_match
                ]
                reason = "pricing_data[state_match & county_match & zip_code_null & apn_section_null & acreage_match]"

            elif not pricing_data[
                state_match
                & county_match
                & zip_code_null
                & apn_section_null
                & acreage_null
            ].empty:
                results = pricing_data[
                    state_match
                    & county_match
                    & zip_code_null
                    & apn_section_null
                    & acreage_null
                ]
                reason = "pricing_data[state_match & county_match & zip_code_null & apn_section_null & acreage_null]"

            elif not pricing_data[
                state_match
                & county_null
                & zip_code_match
                & apn_section_match
                & acreage_match
            ].empty:
                results = pricing_data[
                    state_match
                    & county_null
                    & zip_code_match
                    & apn_section_match
                    & acreage_match
                ]
                reason = "pricing_data[state_match & county_null & zip_code_match & apn_section_match & acreage_match]"

            elif not pricing_data[
                state_match
                & county_null
                & zip_code_match
                & apn_section_match
                & acreage_null
            ].empty:
                results = pricing_data[
                    state_match
                    & county_null
                    & zip_code_match
                    & apn_section_match
                    & acreage_null
                ]
                reason = "pricing_data[state_match & county_null & zip_code_match & apn_section_match & acreage_null]"

            elif not pricing_data[
                state_match
                & county_null
                & zip_code_match
                & apn_section_null
                & acreage_match
            ].empty:
                results = pricing_data[
                    state_match
                    & county_null
                    & zip_code_match
                    & apn_section_null
                    & acreage_match
                ]
                reason = "pricing_data[state_match & county_null & zip_code_match & apn_section_null & acreage_match]"

            elif not pricing_data[
                state_match
                & county_null
                & zip_code_match
                & apn_section_null
                & acreage_null
            ].empty:
                results = pricing_data[
                    state_match
                    & county_null
                    & zip_code_match
                    & apn_section_null
                    & acreage_null
                ]
                reason = "pricing_data[state_match & county_null & zip_code_match & apn_section_null & acreage_null]"

            elif not pricing_data[
                state_match
                & county_null
                & zip_code_null
                & apn_section_match
                & acreage_match
            ].empty:
                results = pricing_data[
                    state_match
                    & county_null
                    & zip_code_null
                    & apn_section_match
                    & acreage_match
                ]
                reason = "pricing_data[state_match & county_null & zip_code_null & apn_section_match & acreage_match]"

            elif not pricing_data[
                state_match
                & county_null
                & zip_code_null
                & apn_section_match
                & acreage_null
            ].empty:
                results = pricing_data[
                    state_match
                    & county_null
                    & zip_code_null
                    & apn_section_match
                    & acreage_null
                ]
                reason = "pricing_data[state_match & county_null & zip_code_null & apn_section_match & acreage_null]"

            elif not pricing_data[
                state_match
                & county_null
                & zip_code_null
                & apn_section_null
                & acreage_match
            ].empty:
                results = pricing_data[
                    state_match
                    & county_null
                    & zip_code_null
                    & apn_section_null
                    & acreage_match
                ]
                reason = "pricing_data[state_match & county_null & zip_code_null & apn_section_null & acreage_match]"

            elif not pricing_data[
                state_match
                & county_null
                & zip_code_null
                & apn_section_null
                & acreage_null
            ].empty:
                results = pricing_data[
                    state_match
                    & county_null
                    & zip_code_null
                    & apn_section_null
                    & acreage_null
                ]
                reason = "pricing_data[state_match & county_null & zip_code_null & apn_section_null & acreage_null]"

            else:
                results = pricing_data[state_match & county_match]
                reason = "pricing_data[state_match & county_match] (no other condition matched)"

            last_row = results.iloc[-1]
            last_row_index = results.index[-1]

            if apn in [
                "209-56-084A",
                "301-29-009",
                "R0028999",
                "219-99-011-H",
                "300-26-028-B",
                "401-42-037-X",
                "503-38-019-D" "503-91-117",
                "506-19-018-D",
                "506-25-025-J",
                "506-33-021-D",
                "509-20-024C",
                "100-01-153G",
                "202-13-137",
            ]:
                print(last_row)
                print(last_row_index)

            # if apn == "00005-00021-017-000":
            #     pricing_data.head().to_json("./pricing_match.json", orient="records")

            self.logger.debug(
                f"After score based matching for {apn} {state} {county} {lot_acreage} {zip_code} {pricing_data.head().to_dict(orient='records') if not pricing_data.empty else pricing_data}"
            )

            return last_row

        except Exception as find_matching_rows:
            self.logger.warn(
                f"Exception {str(find_matching_rows)} while finding matching rows for {state} {county} {zip_code} {apn} {lot_acreage} "
            )
            #  update the mongodb collection which tracks missed properties.
            traceback.print_exc()
            # exit()
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
            # self.logger.debug(str(row["Property_State_Name_Short"]).strip().lower())

            sheet_name = ""

            # Map state abbreviation to corresponding sheet name
            match str(row["Property_State_Name"]).strip().lower():
                case "arizona":
                    sheet_name = "Arizona"
                case "georgia":
                    sheet_name = "Georgia"
                case _:
                    if str(row["Property_State_Name"]).strip().lower() in [
                        "NC",
                        "NORTH CAROLINA",
                        "NORTH-CAROLINA",
                        "NORTH_CAROLINA",
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
                        np.int_,
                        np.float_,
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
                    f.write(writeable)
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
                and isinstance(row["Price_per_Acre"], (int, float, np.int_, np.float_))
                and row["Price_per_Acre"] != 0
                and not pd.isna(row["Lot_Acreage"])
                and isinstance(row["Lot_Acreage"], (int, float, np.int_, np.float_))
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
        elif market_price < 75000:
            return 0.45
        elif market_price < 100000:
            return 0.5
        elif market_price < 150000:
            return 0.525
        else:
            return 0.55

    # Function for offer price based on market price
    def calculate_offer_price(self, market_price, offer_percentage):
        return (market_price * offer_percentage) + market_price

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
        return processed_batch

    # Define a function to generate group numbers and control numbers for each row
    def generate_group_numbers_and_control_numbers_v2(self, group):
        # Extract relevant columns
        state_abv = self._state_abrevs.get(str(group.name[1]).lower(), "000")
        properties_count = len(group)
        state_group_mapping = self.group_number_mapping.get(state_abv)

        if state_group_mapping:
            group_number = (
                state_group_mapping.get(str(properties_count))
                if properties_count < 5
                else state_group_mapping.get("5+", "000")
            )

            control_number = None
            if properties_count <= 100:
                control_number = str(np.random.randint(1, 1000000)).zfill(6)

            # Return the group number and control number as a Pandas Series
            return pd.Series(
                {"Group_Number": group_number, "Control_Number": control_number}
            )
        else:
            # If state_group_mapping is None, return default values
            return pd.Series({"Group_Number": "000", "Control_Number": None})

    # Step 2: Determine group number and control number for each group
    def generate_group_and_control(self, row):
        prop_count = row["property_count"]
        state_tmp = str(row["Property_State_Name"]).strip().upper()

        state = ""

        if state_tmp == "ARIZONA":
            state = "AZ"

        elif state_tmp == "GEORGIA":
            state = "GA"

        elif state_tmp in ["NC", "NORTH CAROLINA", "NORTH-CAROLINA", "NORTH_CAROLINA"]:
            state = "NC"

        # self.logger.debug(state_tmp, " ", state)

        group_number = self.group_number_mapping[state].get(
            str(min(prop_count, 5)), "017"
        )
        control_number = (
            hash(
                (
                    row["Owner_ID"],
                    row["Property_State_Name"],
                    row["Property_County_Name"],
                )
            )
            % 10000
        )  # Example of generating a unique control number
        return pd.Series(
            {"group_number": state + group_number, "control_number": control_number}
        )

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
            self._dataframe["Price_per_Acre"] = np.NAN

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

            # self.logger.debug(self._dataframe.head().to_dict(orient="records"))

            self.logger.debug("add control number and group number")

            # Add Control Number and Mail Groups
            self.logger.debug("Adding control number and group number")

            # Step 1: Group by "Owner id", "property state", and "county" and count properties
            grouped = (
                self._dataframe.groupby(
                    ["Owner_ID", "Property_State_Name", "Property_County_Name"]
                )
                .size()
                .reset_index(name="property_count")
            )

            # Step 3: Apply the function to each row
            grouped[["group_number", "control_number"]] = grouped.apply(
                self.generate_group_and_control, axis=1
            )

            self._dataframe = pd.merge(
                self._dataframe,
                grouped[
                    [
                        "Owner_ID",
                        "Property_State_Name",
                        "Property_County_Name",
                        "group_number",
                        "control_number",
                    ]
                ],
                on=["Owner_ID", "Property_State_Name", "Property_County_Name"],
            )

            self.logger.debug("all export preprocessing done")
            self.logger.debug("exporting as json file")
            file_name = "./exported_version-" + self.processing_start_date + ".csv"

            self._dataframe.to_csv(
                file_name,
                index=False,
            )
            exit(0)

            relative_path = Path(file_name)
            absolute_path_string = str(relative_path.resolve())

            upload_result = self.upload_file_to_cloud_storage_bucket(
                file_name=file_name, file_path=absolute_path_string
            )

            if (
                upload_result is None
                or not isinstance(upload_result, str)
                or (isinstance(upload_result, str) and upload_result.strip() == "")
                or (
                    isinstance(upload_result, str)
                    and upload_result.strip != ""
                    and file_name not in upload_result
                )
            ):
                self.upload_log_to_cloud_storage_bucket(
                    file_name=self.drive_reader.log_file_name
                )
                self.upload_log_to_cloud_storage_bucket(file_name=self.log_file_name)
                return {
                    "message": "Failed",
                    "error": "File was generating. Error in uploading to cloud storage",
                }

            self.logger.debug(
                "exported manipulated version, return success status to initiate emil notification"
            )

            signed_url = self.create_signed_url(file_name=file_name)

            Path(relative_path.resolve()).unlink()

            self.upload_log_to_cloud_storage_bucket(
                file_name=self.drive_reader.log_file_name
            )
            self.upload_log_to_cloud_storage_bucket(file_name=self.log_file_name)

            return {
                "message": "success",
                "file_name": file_name,
                "storage_location": upload_result,
                "download_url": signed_url,
            }

        except Exception as preprocessing_exception:
            self.logger.error(f"Exception {str(preprocessing_exception)}")
            traceback.print_exc()

            return {"message": "failed", "error": str(preprocessing_exception)}

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

            destination_directory = "Export_For_Mail_House"

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
                f"Attempting to upload {file_name} existing at {file_path} to cloud storage"
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

    def upload_log_to_cloud_storage_bucket(self, file_name=None):
        try:
            if file_name is None:
                file_name = self.log_file_name
            relative_path = Path(f"./{file_name}")
            file_path = str(relative_path.resolve())

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
            Path(relative_path.resolve()).unlink()
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
    data_frame = pd.read_csv("./raw.csv")

    print(data_frame.head())
    # exit(0)

    b = PropertyRecordsPreProcessor(
        dataframe=data_frame,
    )

    print(b.is_valid_dataframe)

    b.pre_process_fetched_results()
