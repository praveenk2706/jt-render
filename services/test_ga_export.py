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
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

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
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import bigquery


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
        # "Property_State_Name_Short",
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

            # self._dataframe = self._dataframe.drop(
            #     self._dataframe.columns.difference(self._fields_to_include), axis=1
            # )

            self._title_case_columns = [
                "Property_State_Name",
                "Property_County_Name",
                "Owner_First_Name",
                "Owner_Last_Name",
                "Owner_Full_Name",
                "Owner_Mailing_Name",
                "Property_City",
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
                    sheet_name=sheet_name
                )

                # Convert specified columns to strings
                columns_to_convert = ["State", "County", "Zip Code", "APN Section"]
                self._pricing_research_for_state[sheet_name][columns_to_convert] = (
                    self._pricing_research_for_state[sheet_name][
                        columns_to_convert
                    ].astype(str)
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
            ) & (pricing_data["Ending Acreage"].fillna(0) >= lot_acreage)
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

            print(f"Results are {results.empty}")
            print(
                f"Results {results.head(1).to_dict(orient='records') if not results.empty else 'no result'}"
            )

            if not results.empty:

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

            else:
                print(
                    f"results empty for {[apn, state, county, lot_acreage, zip_code]}"
                )

        except Exception as find_matching_rows_exc:
            self.logger.warning(
                f"Exception {str(find_matching_rows_exc)} while finding matching rows for {state} {county} {zip_code} {apn} {lot_acreage} "
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
            # self.logger.debug(str(row["Property_State_Name_Short"]).strip().lower() if )

            sheet_name = ""

            # Map state abbreviation to corresponding sheet name
            match (str(row["Property_State_Name"]).strip().lower()):
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
            match (str(row["Property_State_Name"]).strip().lower()):
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
        elif market_price < 100000:
            return 0.5
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

            self.logger.debug("Randomly breaking the dataframe into testing groups")

            # print(type(self._dataframe))

            updated_batches = self.split_and_process_dataframe_into_mail_groups(
                df=self._dataframe
            )

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
                print(f"updated batches is good")

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

            if (
                file_names is not None
                and isinstance(file_names, list)
                and len(file_names) > 0
            ):

                for index, file_path in enumerate(file_names):
                    # if index >= 1:
                    # break
                    print(f"File {file_path} {index} upload step")

                    new_file_name = str(file_path).strip("./").strip("/tmp/")

                    relative_path = Path(file_path)
                    if Path(file_path).exists():

                        absolute_path_string = str(relative_path.resolve())

                        upload_result = self.upload_file_to_cloud_storage_bucket(
                            file_name=new_file_name, file_path=absolute_path_string
                        )

                        if (
                            upload_result is not None
                            and isinstance(upload_result, str)
                            and len(upload_result.strip()) > 0
                        ):
                            print(f"Upload result {file_path} {upload_result}")
                            upload_locations.append(upload_result.strip())

                            self.logger.debug(
                                "exported manipulated version, return success status to initiate email notification"
                            )

                            print(f"Upload locations {file_path}")
                            print(f"creating signed url for {file_path}")

                            signed_url = self.create_signed_url(file_name=new_file_name)

                            signed_urls.append(signed_url)

                            print(
                                f"deleting the exported file for {file_path} {file_path}"
                            )

                            file_names.append(file_path)

                            Path(relative_path.resolve()).unlink()

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

                for index, item in enumerate(missed_properties):

                    self.logger.debug(
                        f"Upload missed properties csv {item} at index {index}"
                    )

                    (
                        missed_properties_file_path_absolute_string,
                        missed_properties_file_name,
                    ) = (item[0], item[1])

                    missed_properties_upload_result = (
                        self.upload_file_to_cloud_storage_bucket(
                            file_name=missed_properties_file_name,
                            file_path=missed_properties_file_path_absolute_string,
                        )
                    )

                    print(
                        f"Missed properties upload result {missed_properties_upload_result}"
                    )

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


account = {
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
if __name__ == "__main__":

    print(f"fetch data from bigquery")

    dataset = "Dev_Dataset_1"
    table = "Property_Base_2"

    project_id = "property-database-370200"
    new_cred = service_account.Credentials.from_service_account_info(account)
    client = bigquery.Client(credentials=new_cred, project=project_id)

    query_job = client.query(
        f"SELECT * FROM `{project_id}.{dataset}.{table}` WHERE Mail_State='GA' AND Owner_ID IS NOT NULL LIMIT 7500"
    )

    print("query job created")

    print("get data frame")

    dataframe = query_job.to_dataframe()

    print("dataframe")

    if (
        dataframe is None
        or not isinstance(dataframe, pd.DataFrame)
        or (isinstance(dataframe, pd.DataFrame) and dataframe.empty)
    ):
        print(f"Invalid dataframe is None? {dataframe is None}")
        if not isinstance(dataframe, pd.DataFrame):
            print(f"Dataframe  {type(dataframe)}")
            print(dataframe)
        else:
            print(dataframe)

        exit(0)

    print(f"valid dataframe {dataframe.head(1).to_dict(orient='records')}")

    processor = PropertyRecordsPreProcessor(dataframe=dataframe)

    dataframe["Price_per_Acre"] = np.NAN

    print("find price per acre")

    result = processor.process_batch(dataframe)

    print(f"After find price per acre")

    if result is None:
        print(f"result is None")
        exit(0)

    if not isinstance(result, pd.DataFrame):
        print(f"result is not a dataframe {type(dataframe)}")
        exit(0)

    if isinstance(result, pd.DataFrame) and result.empty:
        print("Empty dataframe")
        exit(0)

    print(f"Valid Result {result.head(1).to_dict(orient='records')}")

    print("adding mail group")

    result["Mailer_Group"] = "GA001"

    info = {
        "name": "GA026",
        "type": "SLI & Contract Front",
        "phone": "(912) 513-4454",
        "brand_name": "Sunset Land Investors",
        "website": "SunsetLandInvestors.com",
        "email": "Contact@SunsetLandInvestors.com",
    }
    result["brand_name"] = info["brand_name"]
    result["website"] = info["website"]
    result["email"] = info["email"]
    result["phone"] = info["phone"]
    result["type"] = info["type"]
    result["signature"] = "signature.png"
    # result["control_number"] = control_number
    result["Property_State_Name"] = result["Property_State_Name"].str.title()
    result["Property_County_Name"] = result["Property_County_Name"].str.title()

    print("adding control numbers")
    result["control_number"] = None

    population_size = 999999
    batch_size = len(result)
    sample_size = min(population_size, batch_size)
    control_numbers = random.sample(
        range(1, max(population_size, batch_size)), sample_size
    )

    # Group by specified columns and assign control numbers
    for name, group in result.groupby(
        ["Owner_ID", "Property_State_Name", "Property_County_Name"]
    ):
        control_number = random.choice(control_numbers)
        result.loc[group.index, "control_number"] = control_number

    print(result.head(1).to_dict(orient="records"))

    print("export result")
    # processor.missed_properties_csv
    result.to_csv("test_ga_new.csv", index=False)

    # data_frame = pd.read_csv("./raw.csv")

    # print(data_frame.head())
    # exit(0)

    # b = PropertyRecordsPreProcessor(
    #     dataframe=data_frame,
    # )

    # print(b.is_valid_dataframe)

    # results = b.pre_process_fetched_results()

    # print(results)
