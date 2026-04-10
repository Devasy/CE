"""Utility functions for bcrypt."""
import bcrypt


def hash_password(password: str) -> str:
    """Hashes the given password using bcrypt."""
    salt = bcrypt.gensalt()
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed_password.decode('utf-8')


def verify_password(password: str, hashed_password: str) -> bool:
    """Verify the given password against the hashed password using bcrypt."""
    try:
        return bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8'))
    except ValueError:
        # Handle the case where the hashed password might be in a different format
        return False
    except Exception:
        # Handle the case where the hashed password might be in a different format
        return False
