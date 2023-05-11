import os
import pandas as pd
import logging
from transform.transform import Transform


class FeatureEng(Transform):

    def __init__(self):
        super(FeatureEng, self).__init__({'data_types': {}})
        self.feature_eng_dir = 'feature_eng'
    def run(self):
        logging.info(f"Running Feature Engineering.")
        for filename in self.get_file_names(f"{self.data_dir}/{self.parquet_files_dir}/{self.stock_data_dir}", 'stock'):
            logging.info(f"Processing file : {filename}.")
            # self.__load_stocks(f"{self.data_dir}/stocks/{filename}")
            self.__feature_eng(f"{self.data_dir}/{self.parquet_files_dir}/{self.stock_data_dir}/{filename}", "stock")
            # break
        logging.info(f"Processing etf files.")
        for filename in self.get_file_names(f"{self.data_dir}/{self.parquet_files_dir}/{self.etf_data_dir}", 'etf'):
            logging.info(f"Processing file : {filename}.")
            self.__feature_eng(f"{self.data_dir}/{self.parquet_files_dir}/{self.etf_data_dir}/{filename}", 'etf')
            # break

    def get_file_names(self, dir, typ):
        if not self.is_parq_direct_created:
            self.create_sub_directory(f"{self.data_dir}/{self.parquet_files_dir}/{self.feature_eng_dir}/{self.stock_data_dir}")
            self.create_sub_directory(f"{self.data_dir}/{self.parquet_files_dir}/{self.feature_eng_dir}/{self.etf_data_dir}")
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


    def __feature_eng(self, dir, typ):

        df = pd.read_parquet(dir)
        df.sort_values(by=['Date'], ascending=[True], inplace=True)
        try:
            df['vol_moving_avg'] = df['Volume'].rolling(window=30).mean()
        except Exception as err:
            logging.error(f"Failed to add volume moving average with {err}.")

        try:
            df['adj_close_rolling_med'] = df['Close'].rolling(window=30).median()
        except Exception as err:
            logging.error(f"Failed to add rolling median with {err}.")

        filename = dir.split('/')[-1]
        stocksymb = filename.split('.')[0]

        if typ == 'stock':
            parq_output = f"{self.data_dir}/{self.parquet_files_dir}/{self.feature_eng_dir}/{self.stock_data_dir}/{stocksymb}.parquet.gzip"
        elif typ == 'etf':
            parq_output = f"{self.data_dir}/{self.parquet_files_dir}/{self.feature_eng_dir}/{self.etf_data_dir}/{stocksymb}.parquet.gzip"

        df.to_parquet(parq_output)