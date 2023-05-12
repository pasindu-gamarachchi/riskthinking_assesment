from model_server import app
from flask import request
import logging
from model_server.ml_model.ml_model import MLModel
from model_server.input_validator.input_validator import InputValidator


@app.route("/predict")
def predict():
    """
    Predict route for making predictions based on input parameters.

    Query Parameters:
        - vol_mov_avg: The value of vol_moving_avg parameter.
        - adj_close_rolling_med: The value of adj_close_rolling_med parameter.

    Returns:
        - If the input is valid:
            - A JSON response with the predicted volume: {"Predicted Volume": pred_volume}
        - If the input is invalid:
            - An error message with the corresponding status code: {"error": err_msg}

    """
    logging.info("Prediction in progress.")
    vol_mov_avg = request.args.get('vol_moving_avg')
    adj_close_rolling_med = request.args.get('adj_close_rolling_med')
    xgb_model = MLModel()
    inp_validator = InputValidator()
    is_valid, err_msg, stat_code = inp_validator.validate(vol_mov_avg, adj_close_rolling_med)
    if not is_valid:
        return {"error": err_msg}, stat_code
    pred_volume = xgb_model.predict(vol_mov_avg, adj_close_rolling_med)
    return {"Predicted Volume": pred_volume}, 200

@app.route("/test")
def test():
    return {"message": "Ok"}, 200


@app.errorhandler(500)
def error_500(error):
    return {"code": 500,
            "message": "Internal Server Error. "}, 500


@app.errorhandler(404)
def error_404(error):
    return {"code": 404, "message": "URI is not recognized. Please check and try again."}, 404


@app.errorhandler(405)
def error_405(error):
    return {"code": 405, "message": "Request method is not allowed. Please check and try again."}, 405
