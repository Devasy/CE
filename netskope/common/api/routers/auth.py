"""Provides authentication related methods."""

import os
import re
import json
import traceback
import math

from typing import List
import jwt
from datetime import datetime, timedelta, UTC
from jwt import PyJWTError  # noqa: F401
from fastapi import Depends, APIRouter, HTTPException, Security
from fastapi.param_functions import Form
from fastapi.security import (
    OAuth2PasswordBearer,
    SecurityScopes,
)
from fastapi.responses import RedirectResponse
from urllib.parse import urlparse
from onelogin.saml2.auth import OneLogin_Saml2_Auth
from starlette.requests import Request
from multidict import MultiDict

from netskope.common import PASSWORD_PATTERN
from netskope.common.utils import DBConnector, Collections, Logger, bcrypt_utils
from netskope.common.models import User, Token, UserDB
from netskope.common.utils.password_validator import (
    validate_password_against_policy,
    get_password_policy_from_settings,
)
from netskope.common.utils.const import ACCESS_TOKEN_EXPIRE_MINUTES

router = APIRouter()
logger = Logger()
db_connector = DBConnector()
SECRET_KEY = os.environ["JWT_SECRET"]
ALGORITHM = os.environ["JWT_ALGORITHM"]

SRE_IDP_IDENTITY_ID = os.environ.get("SRE_IDP_IDENTITY_ID", None)
SRE_IDP_SSO_URL = os.environ.get("SRE_IDP_SSO_URL", None)
SRE_IDP_SLO_URL = os.environ.get("SRE_IDP_SLO_URL", None)
SRE_IDP_X509_CERT = os.environ.get("SRE_IDP_X509_CERT", None)

oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="/api/auth",
    scopes={
        "me": "Change account settings.",
        "admin": "Admin access.",
        "api": "Client ID generation access.",
        "cte_read": "Read CTE information.",
        "cte_write": "Write CTE information.",
        "cto_read": "Read CTO information.",
        "cto_write": "Write CTO information.",
        "cre_read": "Read CRE information.",
        "cre_write": "Write CRE information.",
        "cls_read": "Read CLS information.",
        "cls_write": "Write CLS information.",
        "edm_read": "Read EDM information.",
        "edm_write": "Write EDM information.",
        "cfc_read": "Read CFC information.",
        "cfc_write": "Write CFC information.",
        "logs": "Read logs",
        "settings_read": "Read settings information.",
        "settings_write": "Write settings information.",
    },
)

SCOPE_MAPPING = {
    "netskope-ce-read": [
        "cte_read",
        "cto_read",
        "cls_read",
        "edm_read",
        "cfc_read",
        "cre_read",
        "settings_read",
        "logs",
    ],
    "netskope-ce-write": [
        "cte_read",
        "cto_read",
        "cls_read",
        "edm_read",
        "cfc_read",
        "cre_read",
        "settings_read",
        "logs",
        "cte_write",
        "cto_write",
        "cls_write",
        "edm_write",
        "cfc_write",
        "cre_write",
        "settings_write",
    ],
    "netskope-ce-api": ["api"],
    "netskope-ce-admin": ["admin"],
    "netskope-cte-read": ["cte_read"],
    "netskope-cte-write": ["cte_read", "cte_write"],
    "netskope-cto-read": ["cto_read"],
    "netskope-cto-write": ["cto_read", "cto_write"],
    "netskope-cls-read": ["cls_read"],
    "netskope-cls-write": ["cls_read", "cls_write"],
    "netskope-edm-read": ["edm_read"],
    "netskope-edm-write": ["edm_read", "edm_write"],
    "netskope-cfc-read": ["cfc_read"],
    "netskope-cfc-write": ["cfc_read", "cfc_write"],
    "netskope-cre-read": ["cre_read"],
    "netskope-cre-write": ["cre_read", "cre_write"],
    "netskope-ce-logs": ["logs"],
    "netskope-settings-read": ["settings_read"],
    "netskope-settings-write": ["settings_read", "settings_write"],
}


async def _get_current_user(
    security_scopes: SecurityScopes,
    token: str = Depends(oauth2_scheme),
    allow_on_first_login: bool = False,
) -> User:
    """Get current user.

    Args:
        security_scopes (SecurityScopes): Security scopes required.
        token (str, optional): JWT Token. Defaults to Depends(oauth2_scheme).

    Raises:
        HTTPException: On authorization failure.

    Returns:
        User: The owner of the token.
    """
    credentials_exception = HTTPException(
        status_code=401,
        detail="Could not validate credentials.",
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("username")
        if username is None:
            raise credentials_exception
        scopes = payload.get("scopes", [])
        for scope in security_scopes.scopes:
            if scope not in scopes:
                raise HTTPException(403, "Not enough permissions.")
        from_sso: bool = payload.get("sso", False)
        if from_sso:
            return User(
                username=username,
                scopes=scopes,
                firstLogin=False,
                columns={},
                fromSSO=True,
            )
        else:
            user_dict = db_connector.collection(Collections.USERS).find_one(
                {"username": username}
            )
            if user_dict is None:
                raise HTTPException(401, "Could not authenticate the user.")
            if user_dict["firstLogin"] and not allow_on_first_login:
                raise HTTPException(
                    401, "Change the password before using this endpoint."
                )
            return User(
                username=username,
                scopes=scopes,
                firstLogin=user_dict["firstLogin"],
                columns=user_dict.get("columns", {}),  # column visibility settings
            )
    except PyJWTError:
        raise credentials_exception


async def get_current_user(
    security_scopes: SecurityScopes,
    request: Request,
    token: str = Depends(oauth2_scheme),
):
    """Do not allow first time users."""
    request.state.token = token
    return await _get_current_user(security_scopes, token, allow_on_first_login=False)


async def first_time_user(
    security_scopes: SecurityScopes, token: str = Depends(oauth2_scheme)
):
    """Allow first time users."""
    return await _get_current_user(security_scopes, token, allow_on_first_login=True)


class OAuth2PasswordRequestForm:
    """Credential request form."""

    def __init__(
        self,
        grant_type: str = Form("password", pattern="password|client_credentials"),
        username: str = Form(None),
        password: str = Form(None),
        client_id: str = Form(None),
        client_secret: str = Form(None),
        scope: str = Form(""),
    ):
        """Initialize."""
        self.grant_type = grant_type
        self.username = username
        self.password = password
        self.scopes = scope.split()
        self.client_id = client_id
        self.client_secret = client_secret


def is_weak_password(password):
    """Check if the password is aligned with password policy or not.

    Args:
        password (str): password of the user to check.

    Returns:
        bool: true if password is not aligned with password policy else false.
    """
    if password == "admin":
        return False
    return not bool(re.match(PASSWORD_PATTERN, password))


def _convert_seconds_user_display(total_seconds):
    """Convert the total number of seconds into minutes or hours.

    Args:
        total_seconds (int): The total number of seconds.
    Returns:
        str: The converted time in the format "{hours} hour(s)" if more than 60 minutes,
            or "{minutes} minute(s)" otherwise.
    """
    minutes = math.ceil(total_seconds / 60)

    if minutes > 60:
        remaining_hours = int(minutes / 60)
        remaining_minutes = minutes % 60
        if remaining_minutes > 0:
            return f"{remaining_hours} hour(s) and {remaining_minutes} minute(s)"
        else:
            return f"{remaining_hours} hour(s)"
    else:
        return f"{minutes} minute(s)"


def _lockout_mechanism(
    consecutive_fail_attempts: int,
    current_time: datetime,
    form_data: OAuth2PasswordRequestForm,
) -> None:
    """Implement a lockout mechanism for failed login attempts.

    Args:
        consecutive_fail_attempts (int): The number of consecutive failed login attempts.
        credential_down_time (Optional[datetime]): The time when the credentials will be available again.
        current_time (datetime): The current time.
        form_data (OAuth2PasswordRequestForm): The form data containing the login information.

    Raises:
        HTTPException: If the account is locked or the credentials are invalid.
    """
    consecutive_fail_attempts += 1
    credential_down_time = None
    if consecutive_fail_attempts == 5:
        credential_down_time = current_time + timedelta(minutes=2)
    elif consecutive_fail_attempts == 10:
        credential_down_time = current_time + timedelta(minutes=5)
    elif consecutive_fail_attempts % 5 == 0:
        credential_down_time = current_time + timedelta(hours=6)

    # Update user table.
    if form_data.grant_type == "password":
        error_message = "Incorrect username or password."
        lockout_user = f"User '{form_data.username}'"
        db_connector.collection(Collections.USERS).update_one(
            {"username": form_data.username},
            {
                "$set": {
                    "consecutiveFailedAttempts": consecutive_fail_attempts,
                    "credentialDownTime": credential_down_time,
                }
            },
        )
    else:
        error_message = "Invalid client_id/client_secret provided."
        lockout_user = f"Client ID '{form_data.client_id}'"
        db_connector.collection(Collections.USERS).update_one(
            {"tokens.client_id": form_data.client_id},
            {
                "$set": {
                    "tokens.$.consecutiveFailedAttempts": consecutive_fail_attempts,
                    "tokens.$.credentialDownTime": credential_down_time,
                }
            },
        )

    if credential_down_time:
        time_difference = (credential_down_time - current_time).total_seconds()
        user_display_time_difference = _convert_seconds_user_display(time_difference)
        message = (
            "{} has been locked due to multiple login failures."
            " Please retry after {}."
        )
        error_message = message.format("This account", user_display_time_difference)
        lockout_message = message.format(lockout_user, user_display_time_difference)
        logger.error(lockout_message)
    raise HTTPException(401, error_message)


@router.post(
    "/auth",
    response_model=Token,
    tags=["Authentication"],
    description="Get authentication token.",
)
async def get_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """Generate new authentication token.

    Args:
        form_data (OAuth2PasswordRequestForm, optional): Form data. Defaults to Depends().

    Returns:
        Token: Generated authentication token.
    """
    try:
        password_policy_violation = False
        if form_data.grant_type == "password":
            if form_data.username and form_data.password:
                user = db_connector.collection(Collections.USERS).find_one(
                    {"username": form_data.username}, {"_id": 0}
                )
                if user is None:
                    raise HTTPException(401, "Incorrect username or password.")

                current_time = datetime.now()
                credential_down_time = user.get("credentialDownTime")
                consecutive_fail_attempts = user.get("consecutiveFailedAttempts", 0)

                if credential_down_time and credential_down_time > current_time:
                    time_difference = (
                        credential_down_time - current_time
                    ).total_seconds()
                    raise HTTPException(
                        401,
                        "This account has been locked due to multiple login failures."
                        f" Please retry after {_convert_seconds_user_display(time_difference)}.",
                    )

                if not bcrypt_utils.verify_password(
                    form_data.password, user["password"]
                ):
                    _lockout_mechanism(
                        consecutive_fail_attempts, current_time, form_data
                    )

                is_valid, _ = validate_password_against_policy(
                    form_data.password, form_data.username
                )
                password_policy_violation = not is_valid

                # Reset the lockout attributes
                db_connector.collection(Collections.USERS).update_one(
                    {"username": form_data.username},
                    {
                        "$set": {
                            "consecutiveFailedAttempts": 0,
                            "credentialDownTime": None,
                        }
                    },
                )
            else:
                raise HTTPException(401, "Incorrect username or password.")
        elif form_data.grant_type == "client_credentials":
            if form_data.client_id and form_data.client_secret:
                user = db_connector.collection(Collections.USERS).find_one(
                    {"tokens": {"$elemMatch": {"client_id": form_data.client_id}}},
                    {
                        "tokens": {"$elemMatch": {"client_id": form_data.client_id}},
                        "scopes": 1,
                        "firstLogin": 1,
                        "username": 1,
                    },
                )
                if user is not None:
                    token = user["tokens"][0]
                else:
                    raise HTTPException(
                        401, "Invalid client_id/client_secret provided."
                    )

                client_secret = token.get("client_secret")
                current_time = datetime.now()
                credential_down_time = token.get("credentialDownTime")
                consecutive_fail_attempts = token.get("consecutiveFailedAttempts", 0)

                if credential_down_time and credential_down_time > current_time:
                    time_difference = (
                        credential_down_time - current_time
                    ).total_seconds()
                    raise HTTPException(
                        401,
                        "This account has been locked due to multiple login failures."
                        f" Please retry after {_convert_seconds_user_display(time_difference)}.",
                    )
                if not user or (
                    token["expiresAt"] is not None
                    and datetime.now() > token["expiresAt"]
                    or form_data.client_secret != client_secret
                ):
                    _lockout_mechanism(
                        consecutive_fail_attempts, current_time, form_data
                    )

                # Reset the lockout attributes
                db_connector.collection(Collections.USERS).update_one(
                    {"tokens.client_id": form_data.client_id},
                    {
                        "$set": {
                            "tokens.$.consecutiveFailedAttempts": 0,
                            "tokens.$.credentialDownTime": None,
                        }
                    },
                )
            else:
                raise HTTPException(401, "Invalid client_id/client_secret provided.")
        else:
            raise HTTPException(422, "grant_type not supported.")
        if form_data.scopes:  # verify scopes if any
            for scope in form_data.scopes:
                if scope not in user["scopes"]:
                    raise HTTPException(403, "Unauthorized")
            granted_scopes = form_data.scopes
        else:
            # if no scopes specified; grant all of allowed scopes
            granted_scopes = user["scopes"]
        if not form_data.username:
            data = db_connector.collection(Collections.USERS).find_one(
                {
                    "tokens.client_id": form_data.client_id,
                    "tokens.client_secret": form_data.client_secret,
                },
                {"username": 1},
            )
            form_data.username = data.get("username", None)
        logger.info(f"Authentication token generated for user {form_data.username}.")

        # Include password policy in response for first login or policy violation
        response_data = {
            "access_token": jwt.encode(
                {
                    "username": user["username"],
                    "scopes": granted_scopes,
                    "exp": datetime.now(UTC)
                    + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
                },
                SECRET_KEY,
                ALGORITHM,
            ),
            "token_type": "bearer",
            "scopes": granted_scopes,
            "firstLogin": user["firstLogin"],
            "fromSSO": user.get("fromSSO", False),
            "passwordPolicyViolation": (
                False if user["firstLogin"] else password_policy_violation
            ) if form_data.grant_type == "password" else False,
        }

        # Add password policy if first login or password policy violation
        if (user["firstLogin"] or password_policy_violation) and form_data.grant_type == "password":
            response_data["passwordPolicy"] = get_password_policy_from_settings()

        return response_data
    except ValueError:
        raise HTTPException(400, "Could not verify username and password.")


def prepare_request(request, metaurl, relaystate, samlresponse):
    """Prepare request according to query parameter."""
    parse_url = urlparse(metaurl)
    data = {
        "https": "on" if parse_url.scheme == "https" else "off",
        "http_host": parse_url.netloc,
        "server_port": parse_url.port,
        "script_name": "/",
        "get_data": "",
        "post_data": "",
    }
    valid_query_param = ["sso", "slo", "sls", "acs", "sre", "acssre"]
    is_valid_param = any(
        list(map(lambda i: i in request.query_params, valid_query_param))
    )
    if not is_valid_param:
        logger.error(
            "Invalid query parameter is provided.",
            error_code="CE_1000",
            resolution="""\nEnsure that,\n        1. You have correct 'Redirect URI' from the Settings > General > SSO page.\n        2. Query parameter is from “sso”, “slo”, “sls”, or “acs”.\n""",  # noqa
        )
        raise HTTPException(400, "Invalid query parameter provided.")
    if "acs" in request.query_params or "acssre" in request.query_params:
        data["post_data"] = MultiDict(
            {"RelayState": relaystate, "SAMLResponse": samlresponse}
        )
    if "slo" in request.query_params and request.method == "GET":
        data["get_data"] = MultiDict(
            {"RelayState": relaystate, "SAMLResponse": samlresponse}
        )
    return data


def init_saml_auth(req, ssodata):
    """Check for valid authentication."""
    try:
        sso_settings = {
            "strict": "false",
            "debug": "true",
            "sp": {
                "entityId": ssodata["spEntityId"],
                "assertionConsumerService": {
                    "url": ssodata["spAcsUrl"],
                    "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST",
                },
                "singleLogoutService": {
                    "url": ssodata["spSlsUrl"],
                    "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
                },
                "NameIDFormat": "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress",
                "x509cert": "",
                "privateKey": "",
            },
            "idp": {
                "entityId": ssodata["idpEntityId"],
                "singleSignOnService": {
                    "url": ssodata["idpSsoUrl"],
                    "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
                },
                "singleLogoutService": {
                    "url": ssodata["idpSloUrl"],
                    "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
                },
                "x509cert": ssodata["idpX509Cert"],
            },
            "security": {
                "allowSingleLabelDomains": True,
                "requestedAuthnContext": False,
            },
        }
        auth = OneLogin_Saml2_Auth(req, sso_settings)
    except Exception as err:
        logger.error(
            f"Error occurred while validating SSO parameters: {err}",
            error_code="CE_1121",
            details=traceback.format_exc(),
        )
        raise HTTPException(
            400, "SSO not configured properly. Contact system administrator."
        ) from err
    return auth


class SsosamlRequestForm:
    """Credential response form."""

    def __init__(self, RelayState: str = Form(None), SAMLResponse: str = Form(None)):
        """Initialize."""
        self.RelayState = RelayState
        self.SAMLResponse = SAMLResponse


def _map_sso_scopes(roles: List[str]) -> List[str]:
    scopes = ["me"]
    if not roles:
        return scopes
    roles = roles.pop()
    for role in roles.split(";"):
        if SCOPE_MAPPING.get(role) is not None:
            scopes.extend(SCOPE_MAPPING.get(role, []))
    return list(set(scopes))


@router.post(
    "/ssoauth",
    description="Authentication with ssosaml",
    tags=["Authentication"],
)
def ssoauth(
    request: Request,
    form_data: SsosamlRequestForm = Depends(),
):
    """Perform sso saml actions.

    Args:
        request (Request): Requests class variable.
        form_data (OAuth2PasswordRequestForm, optional): Form data. Defaults to Depends().

    Returns:
        None.
    """
    setting_doc = db_connector.collection(Collections.SETTINGS).find_one({})
    if "sre" in request.query_params or "acssre" in request.query_params:
        base_path = re.findall(r"http.*?(http.*):", request.url._url)[0]
        ssodata = {
            "spEntityId": base_path + "/api/metadata?sre=true",
            "spAcsUrl": base_path + "/api/ssoauth?acssre=true",
            "spSlsUrl": base_path + "/api/slslogout?sre=true",
            "idpEntityId": SRE_IDP_IDENTITY_ID,
            "idpSsoUrl": SRE_IDP_SSO_URL,
            "idpSloUrl": SRE_IDP_SLO_URL,
            "idpX509Cert": SRE_IDP_X509_CERT,
        }
    elif not setting_doc.get("ssoEnable", False):
        raise HTTPException(400, "Ssosaml is not allowed.")
    else:
        ssodata = setting_doc.get("ssosaml", {})
    metaurl = ssodata.get("spEntityId")
    req = prepare_request(
        request, metaurl, form_data.RelayState, form_data.SAMLResponse
    )
    auth = init_saml_auth(req, ssodata)
    path = re.findall(r"(^http.*)\/api", metaurl)[0]
    if "sso" in request.query_params:
        forceAuth = setting_doc.get("forceAuth", False)
        auth_response = auth.login(return_to=path, force_authn=forceAuth)
        if "sre" in request.query_params:
            return RedirectResponse(auth_response, status_code=303)
        return auth_response
    elif "acs" in request.query_params or "acssre" in request.query_params:
        auth.process_response()
        errors = auth.get_errors()
        if not auth.is_authenticated():
            logger.error(
                "Error occurred while authenticating the SSO user.",
                details=json.dumps(
                    {"errors": errors, "last_error": auth.get_last_error_reason()}
                ),
                error_code="CE_1053",
            )
            raise HTTPException(401, "Could not authenticate.")
        if len(errors) == 0:
            data = auth.get_attributes()
            if "username" not in data or "roles" not in data:
                raise HTTPException(
                    401,
                    "Could not authenticate. username/roles attribute not set.",
                )
            username = data["username"].pop()
            logger.info(f"Authentication token generated for user {username}.")
            granted_scopes = _map_sso_scopes(data.get("roles", []))
            access_token = jwt.encode(
                {
                    "username": username,
                    "scopes": granted_scopes,
                    "sso": True,
                    "exp": datetime.now(UTC)
                    + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
                },
                SECRET_KEY,
                ALGORITHM,
            )
            db_connector.collection(Collections.USERS).update_one(
                {"username": username},
                {
                    "$set": UserDB(
                        username=username,
                        scopes=granted_scopes,
                        firstLogin=False,
                        sso=True,
                    ).model_dump()
                },
                upsert=True,
            )
            response = RedirectResponse(path, status_code=303)
            response.set_cookie(key="access_token", value=access_token)
            response.set_cookie(key="scopes", value=json.dumps(granted_scopes))
            response.set_cookie(key="token_type", value="bearer")
            response.set_cookie(key="sso", value=True)
            return response
        else:
            logger.error(
                "Error occurred while processing ssosaml response for the SSO user.",
                details=json.dumps(
                    {"errors": errors, "last_error": auth.get_last_error_reason()}
                ),
                error_code="CE_1054",
            )
            raise HTTPException(401, "Error occurred in ssosaml response.")
    elif "slo" in request.query_params:
        return auth.logout(return_to=path)
    else:
        raise HTTPException(404, "Invalid query parameter.")


@router.get(
    "/slslogout",
    description="Logout response from Onelogin.",
    tags=["Authentication"],
)
async def ssologout(request: Request):
    """Validate logout response.

    Args:
        request (Request): Requests class variable.

    Returns:
        None.
    """
    setting_doc = db_connector.collection(Collections.SETTINGS).find_one({})
    if not setting_doc.get("ssoEnable", False) and "sre" not in request.query_params:
        raise HTTPException(405, "Ssosaml is not allowed.")

    base_path = re.findall(r"http.*?(http.*):", request.url._url)[0]

    if "sre" in request.query_params:
        ssodata = {
            "spEntityId": base_path + "/api/metadata?sre=true",
            "spAcsUrl": base_path + "/api/ssoauth?acssre=true",
            "spSlsUrl": base_path + "/api/slslogout?sre=true",
            "idpEntityId": SRE_IDP_IDENTITY_ID,
            "idpSsoUrl": SRE_IDP_SSO_URL,
            "idpSloUrl": SRE_IDP_SLO_URL,
            "idpX509Cert": SRE_IDP_X509_CERT,
        }
    else:
        ssodata = setting_doc.get("ssosaml", {})
    metaurl = ssodata.get("spEntityId")

    req = prepare_request(
        request,
        metaurl,
        request.query_params["RelayState"],
        request.query_params["SAMLResponse"],
    )
    auth = init_saml_auth(req, ssodata)
    url = auth.process_slo()
    errors = auth.get_errors()
    if len(errors) == 0:
        if url is not None:
            response = RedirectResponse(url, status_code=303)
        else:
            path = re.findall(r"(^http.*)\/api", metaurl)[0]
            response = RedirectResponse(path, status_code=303)
        return response
    else:
        logger.error(
            "Error occurred while logging out the SSO user.",
            details=json.dumps(
                {"errors": errors, "last_error": auth.get_last_error_reason()}
            ),
            error_code="CE_1055",
        )
    return url


@router.get(
    "/metadata",
    description="Get Ssosaml Metadata.",
    tags=["Authentication"],
)
async def read_metadata(request: Request):
    """Read current ssosaml metadata."""
    setting_doc = db_connector.collection(Collections.SETTINGS).find_one({})
    if not setting_doc.get("ssoEnable", False) and "sre" not in request.query_params:
        raise HTTPException(400, "Ssosaml is not allowed.")

    base_path = re.findall(r"http.*?(http.*):", request.url._url)[0]

    if "sre" in request.query_params:
        ssodata = {
            "spEntityId": base_path + "/api/metadata?sre=true",
            "spAcsUrl": base_path + "/api/ssoauth?acssre=true",
            "spSlsUrl": base_path + "/api/slslogout?sre=true",
            "idpEntityId": SRE_IDP_IDENTITY_ID,
            "idpSsoUrl": SRE_IDP_SSO_URL,
            "idpSloUrl": SRE_IDP_SLO_URL,
            "idpX509Cert": SRE_IDP_X509_CERT,
        }
    else:
        ssodata = setting_doc.get("ssosaml", {})
    metaurl = ssodata.get("spEntityId")
    spacsurl = ssodata.get("spAcsUrl")
    spslssurl = ssodata.get("spSlsUrl")
    sso_settings = {
        "sp": {
            "entityId": metaurl,
            "assertionConsumerService": {
                "url": spacsurl,
                "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST",
            },
            "singleLogoutService": {
                "url": spslssurl,
                "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
            },
            "NameIDFormat": "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress",
            "x509cert": "",
            "privateKey": "",
        }
    }
    return sso_settings


@router.get(
    "/management-token",
    tags=["Management"],
    description="New token",
)
async def issue_new_token(
    user: User = Security(
        get_current_user, scopes=["admin"]
    ),
):
    """Issue new token for communication with Management Server."""
    token = jwt.encode(
        {
            "username": user.username,
            "scopes": user.scopes,
            "type": "user-access",
            "exp": datetime.now(UTC)
            + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        },
        SECRET_KEY,
        ALGORITHM,
    )
    logger.info(f"New Management token issued for user {user.username}")
    return token
