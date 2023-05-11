import pandas as pd
import os
import logging


class Transform:

    def __init__(self, data_types={}):
        self.meta_data = None
        self.data_types = data_types
        self.data_dir = os.getenv("DATA_DIR", "data")
        self.meta_data_csv = os.getenv("META_DATA_CSV", "symbols_valid_meta.csv")
        self.stock_data_dir = os.getenv("STOCK_DATA_DIR", "/stocks")
        self.etf_data_dir = os.getenv("ETF_DATA_DIR", "/etfs")
        # self.load_meta_data()
        self.is_parq_direct_created = False
        self.parq_stock_file_names = set()
        self.parq_etf_file_names = set()
        self.parquet_files_dir = os.getenv("PARQ_DIR", "parquet_files")
        self.ml_models_dir = 'models'
        self.feature_eng_dir = 'feature_eng'
        self.existing_ml_models_stock = set()
        self.existing_ml_models_etf = set()

    def load_meta_data(self):
        try:
            self.meta_data = pd.read_csv(f'{self.data_dir}/{self.meta_data_csv}')
        except Exception as err:
            logging.error(f"Failed to load metadata with {err}.")

    def run(self):
        logging.info("Processing stock files.")
        self.load_meta_data()
        for filename in self.get_file_names(f"{self.data_dir}/{self.stock_data_dir}", 'stock'):
            logging.info(f"Processing file : {filename}.")
            self.__load_stocks(f"{self.data_dir}/stocks/{filename}")
            break
        logging.info("Processing etf files.")
        for filename in self.get_file_names(f"{self.data_dir}/{self.etf_data_dir}", 'etf'):
            logging.info(f"Processing file : {filename}.")
            self.__load_stocks(f"{self.data_dir}/etfs/{filename}", 'etf')
            break
    def get_file_names(self, dir, typ):
        if not self.is_parq_direct_created:
            self.create_sub_directory(f"{self.data_dir}/{self.parquet_files_dir}/{self.stock_data_dir}")
            self.create_sub_directory(f"{self.data_dir}/{self.parquet_files_dir}/{self.etf_data_dir}")
            self.is_parq_direct_created = True
        self.get_parq_file_names(typ)
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
            # else:
            #    logging.info(f"{stockfile.split('.')[0]}  has already been converted to a parquet file.")

    def get_parq_file_names(self, typ='stock', level='transform', feature_eng_dir=None):
        logging.info(f"Received level : {level}")
        if level =='transform':
            existing_stock_dir = f'{self.data_dir}/{self.parquet_files_dir}/{self.stock_data_dir}'
            existing_etf_dir = f'{self.data_dir}/{self.parquet_files_dir}/{self.etf_data_dir}'
        elif level =='feature_eng':
            existing_stock_dir = f'{self.data_dir}/{self.parquet_files_dir}/{feature_eng_dir}/{self.stock_data_dir}'
            existing_etf_dir = f'{self.data_dir}/{self.parquet_files_dir}/{feature_eng_dir}/{self.etf_data_dir}'
        elif level == 'ml_models':
            existing_stock_dir = f'{self.data_dir}/{self.ml_models_dir}/{self.stock_data_dir}'
            existing_etf_dir = f'{self.data_dir}/{self.ml_models_dir}/{self.etf_data_dir}'
        if typ == 'stock':
            if level == 'ml_models':
                for ml_model in os.listdir(existing_stock_dir):
                    self.existing_ml_models_stock.add(ml_model.split('.')[0])
            else:
                for parq_file in os.listdir(existing_stock_dir):
                    self.parq_stock_file_names.add(parq_file.split('.')[0])
        elif typ == 'etf':
            if level == 'ml_models':
                for ml_model in os.listdir(existing_stock_dir):
                    self.existing_ml_models_etf.add(ml_model.split('.')[0])
            else:
                for parq_file in os.listdir(existing_etf_dir):
                    self.parq_etf_file_names.add(parq_file.split('.')[0])

    def __load_stocks(self, stock_file, typ='stock'):
        logging.info(f"Loading {stock_file}")
        stocksymb = self.get_stock_etf_symbol(stock_file)
        df = pd.read_csv(f'{stock_file}', dtype=self.data_types)
        stocksymb = stocksymb.split('/')[-1]
        logging.info(f"Loaded dataframe with shape {df.shape[0]}, {df.shape[1]} for stock/etf symbol : {stocksymb}")
        df = self.__add_stock_etf_symbol(df, stocksymb)
        merged_df = df.join(self.meta_data.set_index('Symbol'), on='Symbol', how='left')
        logging.info(f"Merged df shape : {merged_df.shape[0]}, {merged_df.shape[1]}")
        if typ == 'stock':
            parq_output = f"{self.data_dir}/{self.parquet_files_dir}{self.stock_data_dir}/{stocksymb}.parquet.gzip"
        elif typ == 'etf':
            parq_output = f"{self.data_dir}/{self.parquet_files_dir}{self.etf_data_dir}/{stocksymb}.parquet.gzip"
        merged_df.to_parquet(parq_output)
        # return merged_df

    def get_stock_etf_symbol(self, filename):
        try:
            return filename.split('.')[0]
        except Exception as err:
            logging.error(f"{err} getting stockname.")

    def __add_stock_etf_symbol(self, df, stocksymb):
        try:
            df['Symbol'] = stocksymb
            return df
        except Exception as err:
            logging.error(f"Failed to add stock symbol.")
            return df

    def create_sub_directory(self, directory_path):
        """

        """
        directory_list = directory_path.split('/')
        curr_path = ''
        for direct in directory_list:
            curr_path = os.path.join(curr_path, direct)
            self.__create_data_directory(curr_path)

    def __create_data_directory(self, directory_name):
        """
        Create directory.
        :param directory_name:
        :return:
        """
        try:
            if not os.path.exists(directory_name):
                os.makedirs(directory_name)
                logging.info(f"Created directory {directory_name}.")
            else:
                logging.info(f"{directory_name}, already exists.")
        except PermissionError as err:
            logging.error(f"{err} creating directory.")
            try:
                init_mask = os.umask(0)
                os.makedirs(directory_name, 0o77)
            #except Exception as err:
                #logging.warning(f"Failed to create directory after updating permission.")
            finally:
                os.umask(init_mask)
