import logging
import os
import requests
import csv
import zipfile
from datetime import datetime


class DataExtraction:
    """
    A class for performing data extraction operations.

    Methods:
    - run(): Runs the data extraction process.
    - get_timestamp(): Retrieves the current timestamp in a formatted string.
    - get_csv_from_url(): Retrieves a CSV file from a specified URL and saves it locally.
    - unzip_file(file_path, extract_path): Unzips a specified ZIP file to the specified extraction path.
    - __create_data_directory(directory_name): Creates a directory to store ZIP files.

    Note:
    - The class assumes the presence of required libraries such as `os`, `datetime`, `logging`, `zipfile`, and `requests`.
    - Environment variables are used to configure various parameters, such as data directory, ZIP file name, and CSV file URL.
    """

    def run(self):
        """
        Runs the data extraction process.

        This function performs the following steps:
        1. Retrieves the data directory path from the environment variable "DATA_DIR". If not found, uses the default value "data".
        2. Retrieves the ZIP file name from the environment variable "ZIP_FILE_NAME". If not found, uses the default value "stock_data.zip".
        3. Creates the data directory if it does not exist.
        4. Retrieves the CSV file from a URL.
        5. Unzips the downloaded ZIP file and extracts its contents to the data directory.

        Note:
        - The CSV file will be downloaded from a URL specified in the implementation of `get_csv_from_url` method.
        - The ZIP file will be extracted to the data directory specified in the environment variable "DATA_DIR" or the default value.

        """
        logging.info("Running data extraction.")
        csv_data_dir = os.getenv("DATA_DIR", "data")
        zipfile_name = os.getenv("ZIP_FILE_NAME", "stock_data.zip")
        self.__create_data_directory(csv_data_dir)
        self.get_csv_from_url()
        self.unzip_file(f"{csv_data_dir}/{zipfile_name}", f"{csv_data_dir}")

    def get_timestamp(self):
        """
        Retrieves the current timestamp in a formatted string.

        Returns:
        - A formatted timestamp string in the format "YYYYMMDDTHHMMSSZ".

        Example:
        - "20230511T153045Z"

        """
        current_time = datetime.utcnow()
        formatted_time = current_time.strftime("%Y%m%dT%H%M%SZ")
        return formatted_time

    def get_csv_from_url(self):
        """
        Retrieves a CSV file from a specified URL and saves it locally.

        Important Steps:
        1. Retrieve data directory path, ZIP file name, and output file path from environment variables.
        2. Skip download if ZIP file already exists in the data directory.
        3. Initiate the download of the CSV file from the URL.
        4. Write the downloaded file to the output file path.

        Note:
        - The CSV file is downloaded from a specified URL.
        - Data directory, ZIP file name, and output file path can be configured using environment variables.
        - The output file will be overwritten if it already exists.
        """
        logging.info("get csv from url called")
        csv_data_dir = os.getenv("DATA_DIR", "data")
        zipfile_name = os.getenv("ZIP_FILE_NAME", "stock_data.zip")
        output_file = os.getenv("OUTPUT_FILE_PATH", f"{csv_data_dir}/{zipfile_name}")
        time_stamp = self.get_timestamp()
        CSV_URL = f'https://storage.googleapis.com/kaggle-data-sets/541298/1054465/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20230511%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date={time_stamp}&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=2118dfd23e6c04b7b072234588133a99f26872cca976f55d98d6973d5a08bd615040c02dddb490d8a722481cebe0ad029869a58fff941c47d5e2f7501c65577aca6690cd4e3e77d591b78008422cd07d357429a83a57e8967efbc6e58169e6c610c064504e70438599c842909c61901ea989a6b5e6b6542a5d4c8c2716fe375e4b112a891a534f44f4af8d2df3d9b519847bb34a37776e5e36bf0cccaec620ae3a01ee2910d1fa2d1c05122024d3d733fd48e9d9def00104f52faa6402ea530027eb88495538aab424927a3a76fbd919cf7f9451d58844ea7cbcf67b7174e013575191e0f475986e4d57544792eec169c0f20ac27353febb6ad1bd6ffa0b274e'

        if zipfile_name in os.listdir(csv_data_dir):
            logging.info("File has already been downloaded, skipping download.")
        else:
            logging.info("File download initiated.")
            logging.info(f"With csv url :\n\t {CSV_URL}")
            csv_file = requests.get(CSV_URL)
            logging.info(f"Resp code : {csv_file.status_code}")
            with open(output_file, "wb") as f:
                chunks_written = 0
                for chunk in csv_file.iter_content(chunk_size=512):
                    if chunk:
                        f.write(chunk)
                        chunks_written += 1
                        if chunks_written % 10000 == 0:
                            logging.info(f"Written chunk : {chunks_written}")

    def unzip_file(self, file_path, extract_path):
        """
        Unzips a specified ZIP file to the specified extraction path.

        Args:
        - file_path (str): The path to the ZIP file to be extracted.
        - extract_path (str): The path where the contents of the ZIP file will be extracted to.

        The function checks if the expected files are already present in the extraction path. If all the expected files are found, the extraction process is skipped. Otherwise, the function extracts the contents of the ZIP file to the extraction path and logs a message indicating successful extraction.

        Note:
        - The expected files are checked based on their names (case-sensitive) within the extraction path.
        - If any of the expected files are missing, the function will extract all contents of the ZIP file.
        - The function assumes that the ZIP file exists and is accessible.

        """
        expected_files = os.getenv("EXPECTED_FILES", "etfs,stocks,symbols_valid_meta.csv")
        if all(file in os.listdir(extract_path) for file in expected_files.split(',')):
            logging.info("Files have been extracted. Skipping extraction.")
        else:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            logging.info('File successfully unzipped.')

    def __create_data_directory(self, directory_name):
        """
        Create directory to store zip files.
        :param directory_name:
        :return:
        """
        if directory_name not in os.listdir():
            os.makedirs(directory_name)
            logging.info(f"Created directory {directory_name} for zip files.")
        else:
            logging.info(f"{directory_name}, already exists.")
