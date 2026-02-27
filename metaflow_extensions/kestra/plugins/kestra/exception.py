from metaflow.exception import MetaflowException


class KestraException(MetaflowException):
    headline = "Kestra error"


class NotSupportedException(KestraException):
    headline = "Kestra feature not supported"
