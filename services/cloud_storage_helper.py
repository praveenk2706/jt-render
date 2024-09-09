import datetime
import logging
import traceback

from google.cloud import storage
from google.oauth2 import service_account


class CloudStorageHelper:
    def __init__(self, store_location) -> None:
        # bucket_name=bukcet_name = bucket_name
        # self._destination_directory = destination_directory
        # self._file_absolute_path = file_absolute_path
        # self._file_name = file_name

        self.log_file_name = f"/tmp/CloudStorageHelper-{datetime.datetime.now().strftime('%Y-%m-%d')}.log"

        self.logger = logging.getLogger("CloudStorageHelper")
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler = logging.FileHandler(self.log_file_name)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

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

        # self.benchMark = Benchmark(log_file_name="/home/itechnolabs/project/jt-output/benchmark_cloud_storage.log")

    def _create_cloud_storage_client(self):
        """
        Creates a Google Cloud Storage client for handling file uploads and downloads.
        """
        if self._storage_service_account_info is not None:
            self._storage_service_account_project_id = self._storage_service_account_info["project_id"]

            self._storage_credentials = service_account.Credentials.from_service_account_info(info=self._storage_service_account_info)

            self._storage_client = storage.Client(credentials=self._storage_credentials)

    def upload_file_to_cloud_bucket(self, bucket_name, uploadable_file_name, destination_directory="logs", file_path_absolute=None, file_name=None):
        """
        Uploads the log file to a specified bucket in Google Cloud Storage.
        """
        if self._storage_service_account_info is not None:
            # # self.benchMark.start_benchmark("Upload file to cloud storage")

            try:

                # create service account and client
                self._create_cloud_storage_client()

                print("created storage client")

                # uploadable_file_name = str(file_name).strip("/tmp")

                # Upload the log file to the specified bucket

                print(f"uploadable file name {uploadable_file_name}")

                blob = self._storage_client.bucket(bucket_name).blob(f"{destination_directory}/{uploadable_file_name}")

                print(f"created blob {destination_directory}/{uploadable_file_name} {file_name} {file_path_absolute}")

                blob.upload_from_filename(f"{file_path_absolute}")

                print("uploaded")

                self.logger.info(f"File uploaded to {bucket_name}/{destination_directory}/{uploadable_file_name}")

                # # self.benchMark.end_benchmark("Upload file to cloud storage")
                return f"{bucket_name}/{destination_directory}/{uploadable_file_name}"

            except Exception as e:
                self.logger.warning(f"Error uploading log file: {e}")
                traceback.print_exc()
                return None

    def download_all_static_files_from_cloud_to_local_tmp(self, bucket_name):
        """
        Downloads static files from Google Cloud Storage to the local temporary folder.
        """

        if self._storage_service_account_info is not None:
            # # self.benchMark.start_benchmark("0. Download all static files ")

            # create service account and client
            self._create_cloud_storage_client()

            # list all blobs in directory olmstead-property-letters/stage2-function-static-requirements
            succeeded = True
            for blob in self._storage_client.list_blobs(
                bucket_name,
                prefix="stage2-function-static-requirements",
            ):

                try:
                    # save the files to tmp folder
                    self.logger.debug("extract name")

                    self.logger.debug(f"blob.name {blob.name}")

                    name = str(blob.name).split("/")[1]

                    self.logger.debug(f"name {name}")

                    if not name.endswith("/"):

                        self.logger.debug("file download location")
                        file_download_location = "/tmp/" + name
                        self.logger.debug(file_download_location)

                        self.logger.debug("download to file")
                        blob.download_to_filename(file_download_location)
                        self.logger.debug("file downloaded")
                        succeeded = True

                except Exception as nameException:
                    self.logger.error(f"cannot download the file {str(blob.name)}")
                    self.logger.error(nameException)
                    succeeded = False

            # # self.benchMark.end_benchmark("0. Download all static files ")

            return succeeded

    def download_single_file_from_cloud_storage(self, bucket_name, inner_directory, file_name, destination_file_directory="/tmp"):
        try:
            bucket = self._storage_client.get_bucket(bucket_name)
            blob = bucket.blob(f"{inner_directory}/{file_name}")
            destination_file_name = f"/tmp/{file_name}"

            blob.download_to_filename(destination_file_name)

            print("Downloaded storage object {} from bucket {} to local file {}.".format(file_name, bucket_name, destination_file_name))
        except Exception as e:
            print(f"Error {str(e)} while downloading file from cloud storage")
            traceback.print_exc()
            self.logger.error(f"Error {str(e)} while downloading file from cloud storage")
