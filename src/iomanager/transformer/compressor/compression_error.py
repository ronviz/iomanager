from iomanager.exceptions.base_io_excpetion import BaseIOException


class CompressionError(BaseIOException):
    pass


class DecompressionError(BaseIOException):
    pass
