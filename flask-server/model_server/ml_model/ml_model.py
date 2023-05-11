import logging
import os
import pickle

import pandas as pd


class MLModel:
    __FEATURES = ['vol_moving_avg', 'adj_close_rolling_med']

    def __init__(self):
        self.model_path = os.getenv("MODEL_PATH", "model_server/static/A.pkl")
        self.model = None
        self.load_model_from_file()
        # self.gen_df_for_xgboost()

    def load_model_from_file(self):
        try:
            self.model = pickle.load(open(self.model_path, "rb"))
        except Exception as err:
            logging.error(f"{err}, failed to load model.\n Model path : {self.model_path}")

    def gen_df_for_xgboost(self, vol_ma, closing_rol_med):

        df = pd.DataFrame(
            list(zip(
                [float(vol_ma)],
                [float(closing_rol_med)]
            )),
            columns=self.__FEATURES
        )
        logging.info(df)

        return df

    def predict(self, vol_ma, closing_rol_med):
        df = self.gen_df_for_xgboost(vol_ma, closing_rol_med)
        try:
            predicted_vol = self.model.predict(df)
        except Exception as err:
            logging.error(f"{err} predicting volume.")

        predicted_vol = str(list(predicted_vol)[0])
        logging.info(f"Predicted volume : \n\t {predicted_vol}")

        return predicted_vol
