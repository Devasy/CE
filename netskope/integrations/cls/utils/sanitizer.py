"""Sanitizer."""

import datetime
import re
import os


def escaper(special_chars):
    """Escapes the given special characters.

    Args:
        special_chars: The special characters to be escaped

    Returns:
        Escaped special characters
    """
    strip_escaped_re = re.compile(r"\\([{}\\])".format(special_chars))
    do_escape_re = re.compile(r"([{}\\])".format(special_chars))

    def escape(s):
        stripped = strip_escaped_re.sub(r"\1", s)
        return do_escape_re.sub(r"\\\1", stripped)

    return escape


def ensure_in_range(debug_name, min, max, num):
    """To Check whether the given value is in given range or not.

    Args:
        debug_name: The human readable name of the value being verified
        min: Min value of threshold
        max: Max value of threshold
        num: The value to be verified

    Raises:
        Exception in case of value is not in given threshold

    Returns:
        Escaped special characters
    """
    if max is None:
        if min is not None and num < min:
            raise Exception("{}: {} less than {}".format(debug_name, num, min))
    elif min is None:
        if max is not None and num > max:
            raise Exception(
                "{}: {} greater than {}".format(debug_name, num, max)
            )
    elif not min <= num <= max:
        raise Exception(
            "{}: {} out of range {}-{}".format(debug_name, num, min, max)
        )


def int_sanitizer(max=None, min=None):
    """Wrap function for ensuring given value is integer & in given range.

    Args:
        min: Min value of threshold
        max: Max value of threshold

    Raises:
        Exception in case of value other than integer

    Returns:
        Function to sanitize the given integer value
    """

    def sanitize(n, debug_name):
        if not isinstance(n, int):
            raise Exception(
                "{}: Expected int, got {}".format(debug_name, type(n))
            )
        ensure_in_range(debug_name, min, max, n)
        return n

    return sanitize


def float_sanitizer():
    """Wrap function for ensuring the given value is float.

    Raises:
        Exception in case of value other than float

    Returns:
        Function to sanitize the given float value
    """

    def sanitize(n, debug_name):
        if not isinstance(n, float):
            raise Exception(
                "{}: Expected float, got {}".format(debug_name, type(n))
            )
        else:
            return n

    return sanitize


def str_sanitizer(regex_str=".*", escape_chars="", min_len=0, max_len=None):
    """Wrap func to check given value is string & has specific properties.

    Args:
        regex_str: The regex to be matched in given string
        escape_chars: The characters to be escaped in given string
        min_len: The min possible length of given string
        max_len: The max possible length of given string

    Raises:
        Exception in case of value other than string

    Returns:
        Function to sanitize the given string
    """
    regex = re.compile("^{}$".format(regex_str), re.DOTALL)
    escape = escaper(escape_chars)

    def sanitize(s, debug_name):
        if not isinstance(s, str):
            raise Exception(
                "{}: Expected str, got {}".format(debug_name, type(s))
            )
        if not regex.match(s):
            raise Exception(
                "{}: {!r} did not match regex {!r}".format(
                    debug_name, s, regex_str
                )
            )
        if os.getenv("CLS_ENABLE_UTF_8_ENCODING", "false").lower() != "true":
            s = s.encode("unicode_escape").decode("utf-8")
        escaped = escape(s)
        if max_len is None and not min_len:
            return escaped

        byte_len = len(escaped)
        if (max_len is None) and (byte_len < min_len):
            raise Exception(
                "{}: String shorter than {} bytes".format(debug_name, min_len)
            )

        if (max_len is not None) and not min_len <= byte_len <= max_len:
            raise Exception(
                "{}: String length out of range {}-{}".format(
                    debug_name, min_len, max_len
                )
            )
        return escaped

    return sanitize


def datetime_sanitizer():
    """Wrap function to check given value is a valid date time instance.

    Raises:
        UDMTypeError in case of value other than datetime

    Returns:
        Function to sanitize the given datetime value
    """

    def sanitize(t, debug_name):
        if not isinstance(t, datetime.datetime):
            raise Exception(
                "{}: Expected datetime, got {}".format(debug_name, type(t))
            )
        else:
            return str(t.timestamp()).split(".")[0]

    return sanitize


def get_sanitizers():
    """Create the dict for each provided values with its sanitizers.

    Returns:
        Dict object having details of all the available UDM fields and its sanitizers
    """
    # Initialize the sanitizers for different data types
    # ipv4_addr_re = r"\.".join([r"\d{1,3}"] * 4)
    ipv4_addr_re = (
        r"^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4]"
        r"[0-9]|25[0-5])$"
    )
    ipv4_addr = str_sanitizer(ipv4_addr_re)
    IPV4SEG = r"(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])"
    IPV4ADDR = r"(?:(?:" + IPV4SEG + r"\.){3,3}" + IPV4SEG + r")"
    IPV6SEG = r"(?:(?:[0-9a-fA-F]){1,4})"
    IPV6GROUPS = (
        r"(?:" + IPV6SEG + r":){7,7}" + IPV6SEG,
        r"(?:" + IPV6SEG + r":){1,7}:",
        r"(?:" + IPV6SEG + r":){1,6}:" + IPV6SEG,
        r"(?:" + IPV6SEG + r":){1,5}(?::" + IPV6SEG + r"){1,2}",
        r"(?:" + IPV6SEG + r":){1,4}(?::" + IPV6SEG + r"){1,3}",
        r"(?:" + IPV6SEG + r":){1,3}(?::" + IPV6SEG + r"){1,4}",
        r"(?:" + IPV6SEG + r":){1,2}(?::" + IPV6SEG + r"){1,5}",
        IPV6SEG + r":(?:(?::" + IPV6SEG + r"){1,6})",
        r":(?:(?::" + IPV6SEG + r"){1,7}|:)",
        r"fe80:(?::" + IPV6SEG + r"){0,4}%[0-9a-zA-Z]{1,}",
        r"::(?:ffff(?::0{1,4}){0,1}:){0,1}[^\s:]" + IPV4ADDR,
        r"(?:" + IPV6SEG + r":){1,6}:?[^\s:]" + IPV4ADDR,
    )

    ipv6_addr_re = "|".join(["(?:{})".format(g) for g in IPV6GROUPS[::-1]])
    ipv6_addr = str_sanitizer(ipv6_addr_re)
    ip_addr = str_sanitizer(r"(" + ipv6_addr_re + r"|" + ipv4_addr_re + r")")
    mac_addr = str_sanitizer(r"\:".join(["[0-9a-fA-F]{2}"] * 6))

    sanitizers = {
        "IPv4 Address": ipv4_addr,
        "IPv6 address": ipv6_addr,
        "IP Address": ip_addr,
        "MAC Address": mac_addr,
        "Time Stamp": datetime_sanitizer(),
        "Floating Point": float_sanitizer(),
        "Integer": int_sanitizer(),
        "String": str_sanitizer(),
        "String31": str_sanitizer(max_len=31),
        "String40": str_sanitizer(max_len=40),
        "String63": str_sanitizer(max_len=63),
        "String100": str_sanitizer(max_len=100),
        "String128": str_sanitizer(max_len=128),
        "String200": str_sanitizer(max_len=200),
        "String255": str_sanitizer(max_len=255),
        "String1023": str_sanitizer(max_len=1023),
        "String2048": str_sanitizer(max_len=2048),
        "String4000": str_sanitizer(max_len=4000),
        "String8000": str_sanitizer(max_len=8000),
    }

    return sanitizers
