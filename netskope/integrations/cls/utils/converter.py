"""Converter."""

import datetime


def string_converter():
    """Wrap function for converting given value to string.

    Raises:
        Exception in case when value is not string compatible

    Returns:
        Function to convert type of given value to string
    """

    def convert(val, debug_name):
        try:
            return str(val)
        except Exception:
            raise Exception(
                "{}: Error occurred while converting to string".format(
                    debug_name
                )
            )

    return convert


def int_converter():
    """Wrap function for converting given value to integer.

    Raises:
        Exception in case when value is not integer compatible

    Returns:
        Function to convert type of given value to integer
    """

    def convert(val, debug_name):
        try:
            return int(val)
        except Exception:
            raise Exception(
                "{}: Error occurred while converting to integer".format(
                    debug_name
                )
            )

    return convert


def float_converter():
    """Wrap function for converting given value to floating point.

    Raises:
        Exception in case when value is not float compatible

    Returns:
        Function to convert type of given value to float
    """

    def convert(val, debug_name):
        try:
            return float(val)
        except Exception:
            raise Exception(
                "{}: Error occurred while converting to float".format(
                    debug_name
                )
            )

    return convert


def datetime_converter():
    """Wrap function for converting given value to datetime object.

    Raises:
        Exception in case when value is not datetime compatible

    Returns:
        Function to convert type of given value to datetime
    """

    def convert(val, debug_name):
        try:
            return datetime.datetime.fromtimestamp(val)
        except Exception as err:
            raise Exception(
                f"{debug_name}: Error occurred while converting to "
                f"datetime: {err}."
            )

    return convert


def type_converter():
    """To Parse the UDM extension CSV string and creates the dict for data type converters.

    Returns:
        Dict object having details of all the available UDM fields and its type converters
    """
    converters = {
        "Time Stamp": datetime_converter(),
        "Integer": int_converter(),
        "Floating Point": float_converter(),
        "IPv4 Address": string_converter(),
        "IPv6 address": string_converter(),
        "MAC Address": string_converter(),
        "IP Address": string_converter(),
        "String": string_converter(),
        "String31": string_converter(),
        "String40": string_converter(),
        "String63": string_converter(),
        "String100": string_converter(),
        "String128": string_converter(),
        "String200": string_converter(),
        "String255": string_converter(),
        "String1023": string_converter(),
        "String2048": string_converter(),
        "String4000": string_converter(),
        "String8000": string_converter(),
    }

    return converters
