import logging
import os
import requests
import csv


class DataExtraction:

    def __init__(self):
        pass

    def run(self):
        logging.info(f"Running data extraction.")
        csv_data_dir = os.getenv("DATA_DIR", "data")
        self.__create_data_directory(csv_data_dir)
        self.temp()
        self.get_csv_from_url()

    def temp(self):
        logging.info(f"Current directory : {os.getcwd()}")
        logging.info(f"List Directory : {os.listdir()}")
        resp = requests.get('https://www.google.com/')
        logging.info(f"Status code for google.com : {resp.status_code}")

    def get_csv_from_url(self):
        logging.info(f"get csv from url called")
        csv_data_dir = os.getenv("DATA_DIR", "data")
        zipfile_name = os.getenv("ZIP_FILE_NAME", "stock_data.zip")
        output_file = os.getenv("OUTPUT_FILE_PATH", f"{csv_data_dir}/{zipfile_name}")
        CSV_URL = 'https://storage.googleapis.com/kaggle-data-sets/541298/1054465/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com@kaggle-161607.iam.gserviceaccount.com/20230505/auto/storage/goog4_request&X-Goog-Date=20230505T200133Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=74b3d7b85d780828d0fa2a2c5f9da96b72394d28ae517782a5806ffc8c45eb28ce5e5089ecbd4433c363ffad32db26c026febf3f2898a51786b0971d8025c0ccdc49993d37a06bb7b592f30de76ba077d2cc501ab47c6389149da4ed3ecc354e623b89a73aac17cbb3c0a1446f7fc10899858a136a9734c82dbe87df8ac28790afaf461379c8846c0450e94c3e42072c13da513cd40acbb4834ce0bbbef124eb6a306ada6a2f82b00e6e32ee164eed8c2775110fa89cb3f6161aed6f1661109eab636bcdb595814108dfe199762a7a741d0e151bf620b0b1c44dfdff0e151e637c131fb0f160c9b978aa201f0fac3d5007fe685fcfb744397335924eaf3829d1'
        download_file = False
        if zipfile_name in os.listdir(csv_data_dir):
            logging.info(f"File has already been downloaded, skipping download.")
        else:
            logging.info(f"File download initiated.")
            if download_file:
                csv_file = requests.get(CSV_URL)
                logging.info(f"Resp code : {csv_file.status_code}")
                with open(output_file, "wb") as f:
                    chunks_written = 0
                    for chunk in csv_file.iter_content(chunk_size=512):
                        if chunk:
                            f.write(chunk)
                            chunks_written += 1
                            if chunks_written%10000==0:
                                logging.info(f"Written chunk : {chunks_written}")

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