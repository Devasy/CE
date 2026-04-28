"""Check disk free alarm for rabbitmq."""

import shutil
import math
import os
import traceback
from netskope.common.utils import Logger, Notifier
from datetime import datetime, UTC
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from netskope.common.utils.rabbitmq_helper import make_rabbitmq_api_call
from netskope.common.utils.db_connector import DBConnector, Collections
from netskope.common.utils.const import (
    MONGODB_RABBITMQ_CERT_LOCATION,
    MONGODB_RABBITMQ_CERT_BANNER_ID,
    UI_CERT_LOCATION,
    UI_CERT_BANNER_ID,
)

logger = Logger()
notifier = Notifier()
db_connector = DBConnector()


def check_disk_free_alarm():
    """Check rabbitmq disk_free_alarm."""
    try:
        response = make_rabbitmq_api_call("/api/nodes")
        for node in response:
            if node.get("disk_free_alarm", False) is True:
                return True
    except Exception:
        pass
    return False


# This function is used for On-prem deployment only.
def get_available_disk_space():
    """Check physical disk space."""
    if os.environ.get("HA_IP_LIST"):
        try:
            total_storage = shutil.disk_usage("/var/lib/rabbitmq").total
            response = make_rabbitmq_api_call("/api/nodes")
            free_storage = math.inf
            for node in response:
                free_storage = min(node["disk_free"], free_storage)
        except Exception:
            total_storage = shutil.disk_usage("/var/lib/rabbitmq").total
            free_storage = shutil.disk_usage("/var/lib/rabbitmq").free
    else:
        total_storage = shutil.disk_usage("/var/lib/rabbitmq").total
        free_storage = shutil.disk_usage("/var/lib/rabbitmq").free
    return math.floor((free_storage / total_storage) * 100)


def check_certs_validity():
    """Check validity of containers certificates."""
    # Check MongoDB and RabbitMQ certificate
    try:
        expiry_date = _post_or_ack_certificate_banner(
            "MongoDB and RabbitMQ",
            MONGODB_RABBITMQ_CERT_LOCATION,
            MONGODB_RABBITMQ_CERT_BANNER_ID,
        )
        _store_cert_expiry("mongodb_rabbitmq", expiry_date)
    except Exception:
        logger.error(
            "Error occurred while checking MongoDB and RabbitMQ certificate.",
            error_code="CE_1071",
            details=traceback.format_exc(),
        )
        notifier.update_banner_acknowledged(MONGODB_RABBITMQ_CERT_BANNER_ID, True)
        _store_cert_expiry("mongodb_rabbitmq", None)

    # Check UI certificate
    try:
        if os.environ.get("UI_PROTOCOL", "https").lower() != "http":
            expiry_date = _post_or_ack_certificate_banner("UI", UI_CERT_LOCATION, UI_CERT_BANNER_ID)
            _store_cert_expiry("ui", expiry_date)
        else:
            _store_cert_expiry("ui", None)
    except Exception:
        logger.error(
            "Error occurred checking UI certificate.",
            error_code="CE_1073",
            details=traceback.format_exc(),
        )
        notifier.update_banner_acknowledged(UI_CERT_BANNER_ID, True)
        _store_cert_expiry("ui", None)


def _store_cert_expiry(cert_type, expiry_date):
    """Store certificate expiry information in database.

    Args:
        cert_type (str): Type of certificate ('ui' or 'mongodb_rabbitmq')
        expiry_date (datetime): Certificate expiry date or None
    """
    try:
        db_connector.collection(Collections.SETTINGS).update_one(
            {},
            {"$set": {f"certExpiry.{cert_type}": expiry_date}},
        )
    except Exception:
        logger.error(
            f"Error storing {cert_type} certificate expiry.",
            details=traceback.format_exc(),
        )


def _post_or_ack_certificate_banner(container_name, cert_path, banner_id):
    """Extract expiry date from a certificate file.

    Args:
        cert_path (str): Path to the certificate file
    Returns:
        datetime: Certificate expiry date in UTC timezone, or None if unable to parse
    Raises:
        FileNotFoundError: If certificate file doesn't exist
        Exception: If certificate cannot be parsed
    """
    current_time = datetime.now(UTC)

    if not os.path.exists(cert_path):
        raise FileNotFoundError(f"Certificate file not found: {cert_path}")

    try:
        with open(cert_path, "rb") as cert_file:
            cert_data = cert_file.read()

        # Parse the certificate
        cert = x509.load_pem_x509_certificate(cert_data, default_backend())

        # Get the expiry date and ensure it's in UTC
        expiry_date = cert.not_valid_after_utc

        # Check if the certificate is already expired
        if expiry_date < current_time:
            error_message = (
                f"The certificate for {container_name} has already expired. "
                "Please renew it by navigating to Settings → General page in the Netskope Cloud Exchange."
            )
            logger.error(error_message, error_code="CE_1073")
            notifier.banner_error(banner_id, error_message)
            return expiry_date

        # Check if the certificate expires within the next 30 days
        if (expiry_date - current_time).days <= 30:
            warning_message = (
                f"The certificate for {container_name} will expire in {(expiry_date - current_time).days + 1} days. "
                "Please renew it by navigating to Settings → General page in the Netskope Cloud Exchange."
            )
            logger.warn(warning_message, error_code="CE_1074")
            notifier.banner_warning(banner_id, warning_message)
            return expiry_date

        notifier.update_banner_acknowledged(banner_id, True)
        return expiry_date
    except Exception as e:
        raise Exception(f"Failed to parse certificate {cert_path}: {str(e)}")
