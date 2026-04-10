"""Netskope common package."""

RABBITMQ_QUEUE_NAME = "cloudexchange"
RABBITMQ_QUORUM_QUEUE_NAME = "cloudexchange_{}"

PASSWORD_PATTERN = (
    r'^(?=.*[a-z])'  # At least one lowercase letter
    r'(?=.*[A-Z])'   # At least one uppercase letter
    r'(?=.*\d)'      # At least one numeric digit
    r'(?=.*[@#$!%^&*|`()_+{}\[\]:;<>,.?~\\/-])'   # At least one special character
)
