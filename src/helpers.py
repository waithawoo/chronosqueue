def validate_type(param, expected_type, param_name):
    if not isinstance(param, expected_type):
        raise TypeError(f"Expected {expected_type.__name__} for {param_name}, but got {type(param).__name__}")