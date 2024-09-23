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
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler = logging.FileHandler(self.log_file_name)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        self._storage_service_account_info = {
            "type": "service_account",
            "project_id": "mail-engine-411414",
            "private_key_id": "47a68d2a4f70ce1ce0397dcc8d7a17deeeec3e8c",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCMn6IfM7L5vaNM\nbfv6ZKV/ER2KGQO8AnmBf924/rcFvzT/aRX+wfQzvDYsw/f5h/3UGv/LkBfDrY5/\nNyk6/l49//sp6rrO3YDCuwGyiLHIbgFm3VRPIREkyI/WIV6jEp0As7nwqFnPPYTl\nYL5uW+Stx83ngpeWmPBcl0hyz+oYSP9XhXJBTaJBXPSgY/waxzY81SUOIoPhqV96\nWjlc32yf5U7sRRvEMSxP/eqr1AeTOcxMdFQyB1K5odNDVKCY2f2Ad/993/I64yBR\n0upcXyW0DseQ7sdeiPTknaenAneBzsquVw+8s7eyocOqNXmDEFx2BAClxLpbUW4L\n+wsa3Y4vAgMBAAECggEAFIhd0oLTerNjC86xW026e8kNevO3v7TNdghWtmN8E3ro\nQPTgm8qedAwNYvrtkxdKtjTdAvrpUA6K6cNYTKoF1mAGld0VZWLNTAxPLU4tRe/e\nBN2DU8HRxYECvjLaz80bdJX0K+dWTwlosASwuYocY/gdIKpzpNnJmtNnZjA6j6H9\njdgD83IftAweviYQ+HzmuxgJe+TbldecDkeUf1L31h/8scZ5bF3t/HUGQOiqHvQR\nzTdVIUV4SGZO5MEUOGoruntafx7nwyz8eg3xgriLE1RjjMZJdASKnJFmPlrVEAnp\nfXlFOZ6lJQzXirCxF7Gd8OyuIxokD+tQt2sYfPXUqQKBgQDCcWdCuL8onndmKTIk\ne1zMjlgXdEmuE4hC5C2DuUojeiv8KxiFXaM285oPQuSfWYWucmm8pnbGjtaDcROM\nstE+7U5SclK/6L7FziyxzRzCmffiqlsIR180jYDdXQcIqbrCANTwF4Hsd/HUwrsy\nFA5NSGIBnXRsDcmcXUkP2JNsGQKBgQC5JHH0GoRE/daDh6zI+or6blr5AyGxI87n\nV8+LjiaZ/Jr+v7dSNUIFS4to22qKIrfPgOp+NAcJqsRpJBYvYe/umcArNOHpadaQ\n+z+3dejo2/4/0uw86J9ll7KHjEAxnYNXfCtne7/SHOF424fkm7Asx2T/qHcQ5AM/\nWAxyp32VhwKBgAtdAUHe9hzzldq8kZecNgImV3OXci7gKVhvaVGHZJPKMgPcqOQy\nYrV1aw11RVMisjNNsGw79jkIY/TuiMvyL1RUHf2nfLqGaY+5ytbhO8sLoHb4qbVE\nJoY1ttA4/Y4+DRIFZmxZk1g0ckeS0cnNbkCJ+GBLyV//NkcTFI/+gi8RAoGBAJ7U\njEoNAZIqbgBSwcF9ZwMm7zaXIC1Weebv5yZjIeySz+liUYPeGrBuOcAQFFtDI4uJ\nniH8wljhvjQw+DNYAr0f+8NVT8WtvZyNXwElz1UNfhYE/hPWwQAn3sn4YQ3vgkDo\nTlgtyscXbbsAIpnVkpMn0Baf0N2Vrs+F2s/1roA7AoGBAIG5JzChVHn5MVqj9sVN\nem6Wfgv5XQHEQS+2H4ZpSjS0hUia8im84WCtOufZNOj9H3sD/R48m362JndT9fok\nmwwb5rNSjLtWmntY95+MPL/AHzqdEMqgidDwP8Vqeifj1CPiQ5aoukcXguQyzneF\nEHZFtgGc4bxzDtQavZeUCJ/Y\n-----END PRIVATE KEY-----\n",
            "client_email": "fresh-service-account@mail-engine-411414.iam.gserviceaccount.com",
            "client_id": "116962641395464571339",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/fresh-service-account%40mail-engine-411414.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com",
        }
        # self.benchMark = Benchmark(log_file_name="/home/itechnolabs/project/jt-output/benchmark_cloud_storage.log")

    def _create_cloud_storage_client(self):
        """
        Creates a Google Cloud Storage client for handling file uploads and downloads.
        """
        if self._storage_service_account_info is not None:
            self._storage_service_account_project_id = (
                self._storage_service_account_info["project_id"]
            )

            self._storage_credentials = (
                service_account.Credentials.from_service_account_info(
                    info=self._storage_service_account_info
                )
            )

            self._storage_client = storage.Client(
                credentials=self._storage_credentials, project="mail-engine-411414"
            )

    def upload_file_to_cloud_bucket(
        self,
        bucket_name,
        uploadable_file_name,
        destination_directory="logs",
        file_path_absolute=None,
        file_name=None,
    ):
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

                blob = self._storage_client.bucket(bucket_name).blob(
                    f"{destination_directory}/{uploadable_file_name}"
                )

                print(
                    f"created blob {destination_directory}/{uploadable_file_name} {file_name} {file_path_absolute}"
                )

                blob.upload_from_filename(f"{file_path_absolute}")

                print("uploaded")

                self.logger.info(
                    f"File uploaded to {bucket_name}/{destination_directory}/{uploadable_file_name}"
                )

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

    def download_single_file_from_cloud_storage(
        self, bucket_name, inner_directory, file_name, destination_file_directory="/tmp"
    ):
        try:
            bucket = self._storage_client.get_bucket(bucket_name)
            blob = bucket.blob(f"{inner_directory}/{file_name}")
            destination_file_name = f"/tmp/{file_name}"

            blob.download_to_filename(destination_file_name)

            print(
                "Downloaded storage object {} from bucket {} to local file {}.".format(
                    file_name, bucket_name, destination_file_name
                )
            )
        except Exception as e:
            print(f"Error {str(e)} while downloading file from cloud storage")
            traceback.print_exc()
            self.logger.error(
                f"Error {str(e)} while downloading file from cloud storage"
            )
