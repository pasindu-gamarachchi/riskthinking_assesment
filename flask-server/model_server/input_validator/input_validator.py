class InputValidator:

    def is_number(self, text_to_val):
        try:
            float(text_to_val)
            return True, "", 200
        except ValueError:
            return False, "Please input a number", 400

    def validate(self, close_ma, rol_median):

        for inp in [close_ma, rol_median]:
            for val_func in [self.is_number]:
                is_valid, err_msg, stat_code = val_func(inp)
                if not is_valid:
                    return is_valid, err_msg, stat_code

        return is_valid, err_msg, stat_code
