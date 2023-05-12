import pandas as pd
import os
import logging


class Transform:
    """
    Performs data transformation tasks on stock and ETF files.

    Attributes:
    - meta_data: Stores the metadata loaded from a CSV file.
    - data_types: A dictionary specifying the data types for the loaded data.
    - data_dir: The directory path where the data is stored.
    - meta_data_csv: The name of the CSV file containing the metadata.
    - stock_data_dir: The directory path for stock data files.
    - etf_data_dir: The directory path for ETF data files.
    - is_parq_direct_created: Flag indicating whether the parquet directory has been created.
    - parq_stock_file_names: Set of parquet stock file names.
    - parq_etf_file_names: Set of parquet ETF file names.
    - parquet_files_dir: The directory path for parquet files.
    - ml_models_dir: The directory path for ML models.
    - feature_eng_dir: The directory path for feature engineering files.
    - existing_ml_models_stock: Set of existing ML models for stocks.
    - existing_ml_models_etf: Set of existing ML models for ETFs.

    Methods:
    - load_meta_data: Loads the metadata from the specified CSV file.
    - run: Executes the data transformation tasks on stock and ETF files.
    - get_file_names: Retrieves the file names from the specified directory based on the file type.
    - get_parq_file_names: Retrieves the parquet file names based on the file type and level.
    - __load_stocks: Loads and transforms the stock or ETF file.
    - get_stock_etf_symbol: Extracts the stock or ETF symbol from the filename.
    - __add_stock_etf_symbol: Adds the stock or ETF symbol as a column in the DataFrame.
    - create_sub_directory: Creates a subdirectory in the specified directory path.
    - __create_data_directory: Creates a directory with the given name.

    Note:
    - This class assumes the presence of certain environment variables for configuration.
    - Logging is used for informational and error messages throughout the class.
    """

    def __init__(self, data_types={}):
        self.meta_data = None
        self.data_types = data_types
        self.data_dir = os.getenv("DATA_DIR", "data")
        self.meta_data_csv = os.getenv("META_DATA_CSV", "symbols_valid_meta.csv")
        self.stock_data_dir = os.getenv("STOCK_DATA_DIR", "/stocks")
        self.etf_data_dir = os.getenv("ETF_DATA_DIR", "/etfs")
        self.is_parq_direct_created = False
        self.parq_stock_file_names = set()
        self.parq_etf_file_names = set()
        self.parquet_files_dir = os.getenv("PARQ_DIR", "parquet_files")
        self.ml_models_dir = 'models'
        self.feature_eng_dir = 'feature_eng'
        self.existing_ml_models_stock = set()
        self.existing_ml_models_etf = set()

    def load_meta_data(self):
        """
        Loads the metadata from a CSV file.

        Reads the CSV file containing metadata and assigns the loaded data to the 'meta_data' attribute of the class.

        Returns:
        - None

        Raises:
        - Exception: If there is an error while loading the metadata.

        Note:
        - This function assumes the presence of the 'data_dir' and 'meta_data_csv' attributes in the class.
        - Logging is used to report any errors encountered during the loading process.
        """
        try:
            self.meta_data = pd.read_csv(f'{self.data_dir}/{self.meta_data_csv}')
        except Exception as err:
            logging.error(f"Failed to load metadata with {err}.")

    def run(self):
        logging.info("Data Transformation Initiated.")
        logging.info("Processing stock files.")
        is_process_all_files = os.getenv("IS_RUN_ALL_FILES", "False")
        is_process_all_files = is_process_all_files.lower() in ("true")
        self.load_meta_data()
        for filename in self.get_file_names(f"{self.data_dir}/{self.stock_data_dir}", 'stock'):
            logging.info(f"Processing file : {filename}.")
            self.__load_stocks(f"{self.data_dir}/stocks/{filename}")
            if not is_process_all_files:
                break
        logging.info("Processing etf files.")
        for filename in self.get_file_names(f"{self.data_dir}/{self.etf_data_dir}", 'etf'):
            logging.info(f"Processing file : {filename}.")
            self.__load_stocks(f"{self.data_dir}/etfs/{filename}", 'etf')
            if not is_process_all_files:
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
        """
        Retrieves the Parquet file names based on the specified file type and level.

        Args:
            typ (str): The file type. 'stock' for stock files or 'etf' for ETF files. Default is 'stock'.
            level (str): The processing level. Possible values are 'transform', 'feature_eng', or 'ml_models'.
                         Default is 'transform'.
            feature_eng_dir (str): The directory path for feature engineering files. Only applicable when level is 'feature_eng'.

        Returns:
            None

        Note:
            - The function populates the existing_ml_models_stock or existing_ml_models_etf sets based on the file type.
            - The directory path is determined based on the file type and processing level.
            - For 'ml_models' level, the function adds the base file names (without extension) to the corresponding set.
            - For other levels, the function adds the base file names of Parquet files to the corresponding set.

        """
        logging.info(f"Received level: {level}")

        if level == 'transform':
            existing_stock_dir = f'{self.data_dir}/{self.parquet_files_dir}/{self.stock_data_dir}'
            existing_etf_dir = f'{self.data_dir}/{self.parquet_files_dir}/{self.etf_data_dir}'
        elif level == 'feature_eng':
            existing_stock_dir = f'{self.data_dir}/{self.parquet_files_dir}/{feature_eng_dir}/{self.stock_data_dir}'
            existing_etf_dir = f'{self.data_dir}/{self.parquet_files_dir}/{feature_eng_dir}/{self.etf_data_dir}'
        elif level == 'ml_models':
            existing_stock_dir = f'{self.data_dir}/{self.ml_models_dir}/{self.stock_data_dir}'
            existing_etf_dir = f'{self.data_dir}/{self.ml_models_dir}/{self.etf_data_dir}'

        file_names_set = self.existing_ml_models_stock if typ == 'stock' else self.existing_ml_models_etf
        directory = existing_stock_dir if typ == 'stock' else existing_etf_dir

        if level == 'ml_models':
            for ml_model in os.listdir(directory):
                file_names_set.add(ml_model.split('.')[0])
        else:
            for parq_file in os.listdir(directory):
                file_names_set.add(parq_file.split('.')[0])

    def __load_stocks(self, stock_file, typ='stock'):
        """
        Loads and transforms the stock or ETF file.

        Args:
            stock_file (str): The file path of the stock or ETF file to be loaded.
            typ (str): The type of file being loaded. 'stock' for stock files or 'etf' for ETF files. Default is 'stock'.

        Returns:
            None

        Note:
            - The function loads the specified stock or ETF file into a DataFrame.
            - It extracts the stock or ETF symbol from the file path.
            - The DataFrame is then merged with the metadata based on the 'Symbol' column.
            - Finally, the merged DataFrame is saved as a Parquet file.

        """
        logging.info(f"Loading {stock_file}")
        stocksymb = self.get_stock_etf_symbol(stock_file)
        df = pd.read_csv(f'{stock_file}', dtype=self.data_types)
        stocksymb = stocksymb.split('/')[-1]
        logging.info(f"Loaded dataframe with shape {df.shape[0]}, {df.shape[1]} for stock/etf symbol : {stocksymb}")
        df = self.__add_stock_etf_symbol(df, stocksymb)
        logging.info(f"Joining stock/etf {stocksymb} to metadata.")
        merged_df = df.join(self.meta_data.set_index('Symbol'), on='Symbol', how='left')
        logging.info(f"Merged df shape : {merged_df.shape[0]}, {merged_df.shape[1]}")
        if typ == 'stock':
            parq_output = f"{self.data_dir}/{self.parquet_files_dir}{self.stock_data_dir}/{stocksymb}.parquet.gzip"
        elif typ == 'etf':
            parq_output = f"{self.data_dir}/{self.parquet_files_dir}{self.etf_data_dir}/{stocksymb}.parquet.gzip"
        merged_df.to_parquet(parq_output)

    @staticmethod
    def get_stock_etf_symbol(filename):
        """
        Extracts the stock or ETF symbol from the given filename.

        Args:
            filename (str): The filename from which to extract the stock or ETF symbol.

        Returns:
            str: The stock or ETF symbol extracted from the filename.

        Raises:
            Exception: If there is an error while extracting the symbol.

        Note:
            - The function assumes that the filename has a specific format where the symbol is separated by a period.
            - It splits the filename based on the period and returns the first part as the symbol.
            - If any error occurs during the extraction process, an exception is raised.
        """
        try:
            return filename.split('.')[0]
        except Exception as err:
            logging.error(f"{err} getting stockname.")

    def __add_stock_etf_symbol(self, df, stocksymb):
        """
        Adds the stock or ETF symbol as a column in the DataFrame.

        Args:
            df (pandas.DataFrame): The DataFrame to which the symbol column will be added.
            stocksymb (str): The stock or ETF symbol to be added as a column.

        Returns:
            pandas.DataFrame: The DataFrame with the symbol column added.

        Raises:
            Exception: If there is an error while adding the symbol column.

        Note:
            - The function adds a new column named 'Symbol' to the DataFrame and assigns the stock or ETF symbol to each row.
            - If any error occurs during the addition process, an exception is raised.
        """
        try:
            df['Symbol'] = stocksymb
        except Exception as err:
            logging.error("Failed to add stock symbol.")
        return df

    def create_sub_directory(self, directory_path):
        """
        Creates a subdirectory in the specified directory path.

        Args:
            directory_path (str): The directory path where the subdirectory will be created.

        Returns:
            None

        Note:
            - The method splits the given directory path using the forward slash ('/') as the separator.
            - It iteratively creates subdirectories by joining the current path with each directory in the path.
            - If the subdirectory already exists, it skips the creation process.
        """
        directory_list = directory_path.split('/')
        curr_path = ''
        for direct in directory_list:
            curr_path = os.path.join(curr_path, direct)
            self.__create_data_directory(curr_path)

    def __create_data_directory(self, directory_name):
        """
        Creates a data directory with the given name.

        Args:
            directory_name (str): The name of the directory to be created.

        Returns:
            None

        Raises:
            PermissionError: If there is an error creating the directory due to permission issues.

        Note:
            - The method checks if the directory does not already exist using the `os.path.exists()` function.
            - If the directory does not exist, it creates the directory using `os.makedirs()` function.
            - The directory is created with the default permission mode.
            - If the directory already exists, it logs a message indicating that it already exists.
            - If there is a permission error while creating the directory, it raises a `PermissionError` and logs an error message.
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
            finally:
                os.umask(init_mask)
