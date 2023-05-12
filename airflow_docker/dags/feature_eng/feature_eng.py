import os
import pandas as pd
import logging
from transform.transform import Transform


class FeatureEng(Transform):
    """
    FeatureEng class for performing feature engineering on stock and ETF data.

    Inherits from the Transform class.

    Attributes:
        feature_eng_dir (str): The directory name for feature engineering.

    Methods:
        __init__(): Initializes a FeatureEng object.
        run(): Executes the feature engineering process.
        get_file_names(dir, typ): Retrieves file names for feature engineering.
        __feature_eng(direct, typ): Performs feature engineering on a given file.

    """

    def __init__(self):
        super(FeatureEng, self).__init__({'data_types': {}})
        self.feature_eng_dir = 'feature_eng'

    def run(self):
        """
        Executes the feature engineering process.

        """
        logging.info("Running Feature Engineering.")
        for filename in self.get_file_names(f"{self.data_dir}/{self.parquet_files_dir}/{self.stock_data_dir}", 'stock'):
            logging.info(f"Processing file : {filename}.")
            self.__feature_eng(f"{self.data_dir}/{self.parquet_files_dir}/{self.stock_data_dir}/{filename}", "stock")
        logging.info("Processing etf files.")
        for filename in self.get_file_names(f"{self.data_dir}/{self.parquet_files_dir}/{self.etf_data_dir}", 'etf'):
            logging.info(f"Processing file : {filename}.")
            self.__feature_eng(f"{self.data_dir}/{self.parquet_files_dir}/{self.etf_data_dir}/{filename}", 'etf')

    def get_file_names(self, dir, typ):
        """
        Retrieves file names for feature engineering.

        Args:
            dir (str): The directory to search for files.
            typ (str): The type of files to retrieve ('stock' or 'etf').

        Yields:
            str: The file names for feature engineering.

        """
        if not self.is_parq_direct_created:
            self.create_sub_directory(f"{self.data_dir}/{self.parquet_files_dir}/"
                                      f"{self.feature_eng_dir}/{self.stock_data_dir}")
            self.create_sub_directory(f"{self.data_dir}/{self.parquet_files_dir}/"
                                      f"{self.feature_eng_dir}/{self.etf_data_dir}")
            self.is_parq_direct_created = True

        self.get_parq_file_names(typ, level='feature_eng', feature_eng_dir=self.feature_eng_dir)
        if typ == 'stock':
            for stockfile in os.listdir(dir):
                if stockfile.split('.')[0] not in self.parq_stock_file_names:
                    logging.info(f"{stockfile.split('.')[0]} not in {self.parq_stock_file_names}")
                    yield stockfile
        elif typ == 'etf':
            for etffile in os.listdir(dir):
                if etffile.split('.')[0] not in self.parq_etf_file_names:
                    logging.info(f"{etffile.split('.')[0]} not in {self.parq_etf_file_names}")
                    yield etffile

    def __feature_eng(self, direct, typ):
        """
        Performs feature engineering on a given file.

        Args:
            direct (str): The path to the file for feature engineering.
            typ (str): The type of the file ('stock' or 'etf').

        """
        df = pd.read_parquet(direct)
        df.sort_values(by=['Date'], ascending=[True], inplace=True)
        try:
            df['vol_moving_avg'] = df['Volume'].rolling(window=30).mean()
            logging.info("Successfully added vol_moving_avg_col.")
        except Exception as err:
            logging.error(f"Failed to add volume moving average with {err}.")

        try:
            df['adj_close_rolling_med'] = df['Close'].rolling(window=30).median()
            logging.info("Successfully added adj_close_rolling_med.")
        except Exception as err:
            logging.error(f"Failed to add rolling median with {err}.")

        filename = direct.split('/')[-1]
        stocksymb = filename.split('.')[0]

        if typ == 'stock':
            parq_output = f"{self.data_dir}/{self.parquet_files_dir}/{self.feature_eng_dir}/" \
                          f"{self.stock_data_dir}/{stocksymb}.parquet.gzip"
        elif typ == 'etf':
            parq_output = f"{self.data_dir}/{self.parquet_files_dir}/{self.feature_eng_dir}/" \
                          f"{self.etf_data_dir}/{stocksymb}.parquet.gzip"

        df.to_parquet(parq_output)
