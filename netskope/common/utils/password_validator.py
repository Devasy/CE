"""Password validation engine."""

import re
from typing import List, Dict, Any, Tuple


class PasswordValidator:
    """Validates a password against set rules."""

    def __init__(self, policy: Dict[str, Any]):
        """Initialize with policy configuration."""
        self.min_length = policy.get("minLength", 8)
        self.max_length = policy.get("maxLength", 72)
        self.require_uppercase = policy.get("requireUppercase", True)
        self.require_lowercase = policy.get("requireLowercase", True)
        self.require_digits = policy.get("requireDigits", True)
        self.require_special_chars = policy.get("requireSpecialChars", True)

    def validate(self, password: str) -> Tuple[bool, List[str]]:
        """Validate the password against set rules."""
        errors = []

        # Length validation
        common_phrase = "Password must contain at"
        if len(password) < self.min_length:
            errors.append(
                f"{common_phrase} least {self.min_length} characters long."
            )
        if len(password) > self.max_length:
            errors.append(f"{common_phrase} most {self.max_length} characters.")

        # Character requirements
        if self.require_uppercase and not re.search(r"[A-Z]", password):
            errors.append(f"{common_phrase} least one uppercase letter.")
        if self.require_lowercase and not re.search(r"[a-z]", password):
            errors.append(f"{common_phrase} least one lowercase letter.")
        if self.require_digits and not re.search(r"\d", password):
            errors.append(f"{common_phrase} least one digit.")
        if self.require_special_chars and not re.search(
            r'[!@#$%^&*()_+\-=\[\]{}|;:,.<>?/~`\'"]', password
        ):
            errors.append(f"{common_phrase} least one special character.")

        return len(errors) == 0, errors


def get_default_policy() -> Dict[str, Any]:
    """Get default password policy."""
    return {
        "minLength": 8,
        "maxLength": 72,
        "requireUppercase": True,
        "requireLowercase": True,
        "requireDigits": True,
        "requireSpecialChars": True,
    }


def get_password_policy_from_settings():
    """Get password policy from settings collection."""
    from ..utils import DBConnector, Collections

    db_connector = DBConnector()
    settings_doc = db_connector.collection(Collections.SETTINGS).find_one({})
    if not settings_doc or "passwordPolicy" not in settings_doc:
        # Return default policy if not found
        return get_default_policy()
    return settings_doc["passwordPolicy"]


def validate_password_against_policy(password: str, username: str = None):
    """Shared function to validate a password against the current policy.

    Args:
        password (str): The password to validate
        username (str, optional): The username for context

    Returns:
        tuple: (is_valid, errors)
    """
    policy_settings = get_password_policy_from_settings()

    # Convert settings format to validator format
    policy = {
        "minLength": policy_settings.get("minLength", 8),
        "maxLength": policy_settings.get("maxLength", 72),
        "requireUppercase": policy_settings.get("requireUppercase", True),
        "requireLowercase": policy_settings.get("requireLowercase", True),
        "requireDigits": policy_settings.get("requireDigits", True),
        "requireSpecialChars": policy_settings.get("requireSpecialChars", True),
    }

    validator = PasswordValidator(policy)
    is_valid, errors = validator.validate(password)

    return is_valid, errors
