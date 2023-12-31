
class TulonaNotImplementedError(Exception):
    def __init__(self, message: str):
        self.message = message
        self.formatted_message = f"ERROR: {self.message}"
        super().__init__(self.formatted_message)

class TulonaProjectException(Exception):
    def __init__(self, message: str):
        self.message = message
        self.formatted_message = f"ERROR: {self.message}"
        super().__init__(self.formatted_message)

class TulonaInvalidProjectConfigError(Exception):
    def __init__(self, message: str):
        self.message = message
        self.formatted_message = f"ERROR: {self.message}"
        super().__init__(self.formatted_message)

class TulonaUnSupportedExecEngine(Exception):
    def __init__(self, message: str):
        self.message = message
        self.formatted_message = f"ERROR: {self.message}"
        super().__init__(self.formatted_message)
