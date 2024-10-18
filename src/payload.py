class Payload:
    """ 
    Payload Class
        PayloadFormat => [Client ID(auto), Empty Delimiter b'', Payload Header, Payload Type, Payload]
    """
    def __init__(self, delimiter: bytes = b'', header: bytes = b'\x01', payload_type: bytes = b'json', data: bytes = b''):
        for arg, name in [(delimiter,'delimiter'), (header,'header'), (payload_type,'payload_type'), (data,'data')]:
            self._validate_type(arg, bytes, name)
        
        self.delimiter = delimiter
        self.header = header      
        self.payload_type = payload_type  
        self.data = data
    
    def _validate_type(self, param, expected_type, param_name):
            if not isinstance(param, expected_type):
                raise TypeError(f"Expected {expected_type.__name__} for {param_name}, but got {type(param).__name__}")
        
    def to_multipart(self):
        return [self.delimiter, self.header, self.payload_type, self.data]