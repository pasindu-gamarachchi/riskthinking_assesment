import pandas as pd
from transform.transform import Transform
import logging
import os
from sklearn.metrics import mean_squared_error
import xgboost as xgb
import pickle


class MLmodel(Transform):
    __cols_to_keep = ['Volume',
                      'vol_moving_avg',
                      'adj_close_rolling_med', 'Date']
    __FEATURES = ['vol_moving_avg', 'adj_close_rolling_med']
    __TARGET = 'Volume'

    def __init__(self):
        super(MLmodel, self).__init__({'data_types': {}})
        self.ml_models_dir = 'models'
        self.feature_eng_dir = 'feature_eng'
        self.existing_ml_models_stock = set()
        self.existing_ml_models_etf = set()
        self.failed_to_train = set()

    def run(self):
        logging.info("Machine Learning Model training.")
        for filename in self.get_file_names(f"{self.data_dir}/{self.parquet_files_dir}/{self.stock_data_dir}", 'stock'):
            logging.info(f"Processing file : {filename}.")
            try:
                self.__ml_model_train(
                    f"{self.data_dir}/{self.parquet_files_dir}/{self.feature_eng_dir}/{self.stock_data_dir}/{filename}",
                    "stock")
            except Exception as err:
                logging.warning(f"{err} training model.")
                self.failed_to_train.add(filename)
                continue
            break
        logging.info("Processing etf files.")
        for filename in self.get_file_names(f"{self.data_dir}/{self.parquet_files_dir}/{self.etf_data_dir}", 'etf'):
            logging.info(f"Processing file : {filename}.")
            try:
                self.__ml_model_train(
                    f"{self.data_dir}/{self.parquet_files_dir}/{self.feature_eng_dir}/{self.etf_data_dir}/{filename}",
                    "etf")
            except Exception as err:
                logging.warning(f"{err} trainging model")
                self.failed_to_train.add(filename)
                continue
            break
        self.log_failed_to_train_files()

    def log_failed_to_train_files(self):
        logging.info("Failed to train a model for the following files : ")
        for file in self.failed_to_train:
            logging.info(f"\tFailed to train a model for file : {file}")

    def __ml_model_train(self, dir, typ):
        logging.info(f"Reading parquet file from : {dir}")
        df = pd.read_parquet(dir)
        df = df[self.__cols_to_keep]
        logging.info(f"df shape : {df.shape[0]}, {df.shape[1]}")
        train_ind_cutoff = int(df.shape[0] * 0.8)
        df.sort_values(by=['Date'], inplace=True)
        train = df.loc[df.index < train_ind_cutoff]
        test = df.loc[df.index >= train_ind_cutoff]
        reg_model = xgb.XGBRegressor(n_estimators=1000, early_stopping_rounds=100)
        X_train, y_train = train[self.__FEATURES], train[self.__TARGET]
        X_test, y_test = test[self.__FEATURES], test[self.__TARGET]
        reg_model.fit(X_train, y_train, eval_set=[(X_train, y_train), (X_test, y_test)])
        stocksymb = dir.split('/')[-1].split('.')[0]
        if typ == 'stock':
            pickle_file_dir = f"{self.data_dir}/{self.ml_models_dir}/{self.stock_data_dir}/{stocksymb}.pkl"
        elif typ == 'etf':
            pickle_file_dir = f"{self.data_dir}/{self.ml_models_dir}/{self.etf_data_dir}/{stocksymb}.pkl"
        pickle.dump(reg_model, open(pickle_file_dir, 'wb'))
        logging.info(f"Model saved to {pickle_file_dir}")

    def get_file_names(self, dir, typ):
        if not self.is_parq_direct_created:
            self.create_sub_directory(f"{self.data_dir}/{self.ml_models_dir}/{self.stock_data_dir}")
            self.create_sub_directory(f"{self.data_dir}/{self.ml_models_dir}/{self.etf_data_dir}")
            self.is_parq_direct_created = True

        self.get_parq_file_names(typ, level='ml_models')
        if typ == 'stock':
            for stockfile in os.listdir(dir):
                if stockfile.split('.')[0] not in self.existing_ml_models_stock:
                    logging.info(f"{stockfile.split('.')[0]} not in {self.existing_ml_models_stock}")
                    yield stockfile
        elif typ == 'etf':
            for etffile in os.listdir(dir):
                if etffile.split('.')[0] not in self.existing_ml_models_etf:
                    logging.info(f"{etffile.split('.')[0]} not in {self.existing_ml_models_etf}")
                    yield etffile
