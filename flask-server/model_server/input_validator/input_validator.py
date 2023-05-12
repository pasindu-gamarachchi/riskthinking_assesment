class InputValidator:
    """
    A class for validating input values.
    """
    def is_number(self, text_to_val):
        """
        Validates if the given text can be converted to a number.

        Args:
            text_to_val (str): The text to validate.

        Returns:
            tuple: A tuple containing a boolean indicating if the text is a number,
                   an error message (if the text is not a number), and a status code.

        """
        try:
            float(text_to_val)
            return True, "", 200
        except ValueError:
            return False, "Please input a number", 400

    def validate(self, close_ma, rol_median):
        """
        Validates the input values.

        Args:
            close_ma (str): The close moving average value to validate.
            rol_median (str): The rolling median value to validate.

        Returns:
            tuple: A tuple containing a boolean indicating if the input values are valid,
                   an error message (if the input values are not valid), and a status code.

        """

        for inp in [close_ma, rol_median]:
            for val_func in [self.is_number]:
                is_valid, err_msg, stat_code = val_func(inp)
                if not is_valid:
                    return is_valid, err_msg, stat_code

        return is_valid, err_msg, stat_code
