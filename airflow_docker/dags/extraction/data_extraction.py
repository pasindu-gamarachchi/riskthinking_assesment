import logging
import os
import requests
import csv
import zipfile
from datetime import datetime


class DataExtraction:

    def __init__(self):
        pass

    def run(self):
        logging.info(f"Running data extraction.")
        csv_data_dir = os.getenv("DATA_DIR", "data")
        zipfile_name = os.getenv("ZIP_FILE_NAME", "stock_data.zip")
        self.__create_data_directory(csv_data_dir)
        # self.temp()
        self.get_csv_from_url()
        self.unzip_file(f"{csv_data_dir}/{zipfile_name}", f"{csv_data_dir}")

    def temp(self):
        logging.info(f"Current directory : {os.getcwd()}")
        logging.info(f"List Directory : {os.listdir()}")
        resp = requests.get('https://www.google.com/')
        logging.info(f"Status code for google.com : {resp.status_code}")

    def get_timestamp(self):
        current_time = datetime.utcnow()
        formatted_time = current_time.strftime("%Y%m%dT%H%M%SZ")
        return formatted_time

    def get_csv_from_url(self):
        logging.info("get csv from url called")
        csv_data_dir = os.getenv("DATA_DIR", "data")
        zipfile_name = os.getenv("ZIP_FILE_NAME", "stock_data.zip")
        output_file = os.getenv("OUTPUT_FILE_PATH", f"{csv_data_dir}/{zipfile_name}")
        # CSV_URL = 'https://storage.googleapis.com/kaggle-data-sets/541298/1054465/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com@kaggle-161607.iam.gserviceaccount.com/20230505/auto/storage/goog4_request&X-Goog-Date=20230505T200133Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=74b3d7b85d780828d0fa2a2c5f9da96b72394d28ae517782a5806ffc8c45eb28ce5e5089ecbd4433c363ffad32db26c026febf3f2898a51786b0971d8025c0ccdc49993d37a06bb7b592f30de76ba077d2cc501ab47c6389149da4ed3ecc354e623b89a73aac17cbb3c0a1446f7fc10899858a136a9734c82dbe87df8ac28790afaf461379c8846c0450e94c3e42072c13da513cd40acbb4834ce0bbbef124eb6a306ada6a2f82b00e6e32ee164eed8c2775110fa89cb3f6161aed6f1661109eab636bcdb595814108dfe199762a7a741d0e151bf620b0b1c44dfdff0e151e637c131fb0f160c9b978aa201f0fac3d5007fe685fcfb744397335924eaf3829d1'
        # CSV_URL = 'https://storage.googleapis.com/kaggle-data-sets/541298/1054465/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20230511%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20230511T162746Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=2118dfd23e6c04b7b072234588133a99f26872cca976f55d98d6973d5a08bd615040c02dddb490d8a722481cebe0ad029869a58fff941c47d5e2f7501c65577aca6690cd4e3e77d591b78008422cd07d357429a83a57e8967efbc6e58169e6c610c064504e70438599c842909c61901ea989a6b5e6b6542a5d4c8c2716fe375e4b112a891a534f44f4af8d2df3d9b519847bb34a37776e5e36bf0cccaec620ae3a01ee2910d1fa2d1c05122024d3d733fd48e9d9def00104f52faa6402ea530027eb88495538aab424927a3a76fbd919cf7f9451d58844ea7cbcf67b7174e013575191e0f475986e4d57544792eec169c0f20ac27353febb6ad1bd6ffa0b274e'
        time_stamp = self.get_timestamp()
        CSV_URL = f'https://storage.googleapis.com/kaggle-data-sets/541298/1054465/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20230511%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date={time_stamp}&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=a9ed0063f0c4dbdce90217dea4ba8df0d28cc8fe5fe92fdbf3ab866a8b986b2647a6e8abb2b04dfa09f0a4d469d88fd17b20784d327aefb0d1ac4cff6deb9b93fe7be45c6027062513b0a8a0da3c851021ec2d0371e7afa01a9bd2e830e49d44d5cd602a75805df0b73a2b1b9f4aeb7c381132bc5d0c3457c293bb8914172b70619c8ab3864e2ade5f3ce33d1e14d33cc67494b73445f683414a5c287f837b73efcc1abc41fa43198b4c4d2101aea8bab2d30c9eee210b3bd5ce894dbde2add87dba1c387c173ce15f7899a2b27df6536fc4f8eade1ff287e62178d8cced2d476e20c5368ed659b97de82cf4060d33fb6d427f1274e5627f27b2cc934b9e087a'
        CSV_URL = 'https://storage.googleapis.com/kaggle-data-sets/541298/1054465/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20230511%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20230511T162746Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=2118dfd23e6c04b7b072234588133a99f26872cca976f55d98d6973d5a08bd615040c02dddb490d8a722481cebe0ad029869a58fff941c47d5e2f7501c65577aca6690cd4e3e77d591b78008422cd07d357429a83a57e8967efbc6e58169e6c610c064504e70438599c842909c61901ea989a6b5e6b6542a5d4c8c2716fe375e4b112a891a534f44f4af8d2df3d9b519847bb34a37776e5e36bf0cccaec620ae3a01ee2910d1fa2d1c05122024d3d733fd48e9d9def00104f52faa6402ea530027eb88495538aab424927a3a76fbd919cf7f9451d58844ea7cbcf67b7174e013575191e0f475986e4d57544792eec169c0f20ac27353febb6ad1bd6ffa0b274e'

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
