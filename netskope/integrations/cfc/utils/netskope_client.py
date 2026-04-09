"""Netskope Client API wrapper to communicate with CFC tenant APIs."""

import json
import os
from uuid import uuid4

import requests

from netskope.common.models.settings import SettingsDB
from netskope.common.utils import Collections, DBConnector, Logger
from netskope.common.utils.handle_exception import (
    handle_exception,
    handle_status_code
)
from netskope.common.utils.proxy import get_proxy_params
from netskope.common.utils import add_user_agent
from .exceptions import CustomException as NetskopeClientCFCError


class NetskopeClientCFC:
    """API wrapper to communicate with CFC tenant APIs."""

    MAX_STATUS = "skip-max"
    SAME_HASH_STATUS = "skip-same"
    INVALID_FILE_STATUS = "error"
    DATA_KEY = "fingerprints"
    OVER_ALL_STATUS_KEY = "overallStatus"
    VALID_FILES_KEY = "valid"
    MAXIMUM_NUMBER_OF_FILES_PER_CLASSIFIER = 10000

    def __init__(
            self,
            api_token_v2: str,
            tenant_base_url: str,
            plugin: str = None,
    ) -> None:
        """Create wraper to communicate with CFC tenant APIs.

        Args:
            api_token_v2 (str): V2 token for Netskope tenant
            tenant_base_url (str): Netskope tenant base url with http(s)://
            plugin (str, optional): CFC Plugin configuration name. Defaults to None.
        """
        self.headers = add_user_agent({
            'accept': 'application/json',
            'Netskope-Api-Token': api_token_v2
        })
        self.base_url = tenant_base_url.strip("/")
        self.plugin = plugin
        self.logger = Logger()
        self.proxy = self.get_proxy()

    @staticmethod
    def get_proxy() -> dict:
        """Get proxy dict."""
        db_connector = DBConnector()
        settings = db_connector.collection(Collections.SETTINGS).find_one({})
        return get_proxy_params(SettingsDB(**settings))

    def all_custom_classifiers(
            self,
            offset: int = 0,
            limit: int = 0,
            customOnly: bool = False
    ):
        """Get all custom classiers.

        Args:
            offset (int, optional): Offset from where to start listing. Defaults to 0.
            limit (int, optional): Limit to which classifiers will be listed. Defaults to 0.
            customOnly (bool, optional): Should only return custom classifiers only. Defaults to False.
        """
        params = {
            "url": f"{self.base_url}/api/v2/services/dlp/classifiers/custom",
            "params": {
                "offset": offset,
                "limit": limit,
                "customOnly": json.dumps(customOnly)
            },
            "headers": self.headers,
            "proxies": self.proxy
        }
        success, response = handle_exception(
            requests.get,
            error_code="CFC_1028",
            custom_message="Error ocuured while fetching custom classifiers.",
            plugin=self.plugin,
            **params
        )
        if not success:
            raise response
        parsed_response = handle_status_code(
            response=response,
            error_code="CFC_1030",
            plugin=self.plugin,
            notify=True,
            parse_response=True,
            handle_forbidden=True,
        )
        return parsed_response

    def classifier_by_id(
            self,
            class_id: str
    ):
        """Fetch classifier by id.

        Args:
            class_id (str): Custom/Overlay classifier id
        """
        params = {
            "url": f"{self.base_url}/api/v2/services/dlp/classifiers/custom/{class_id}",
            "headers": self.headers,
            "proxies": self.proxy
        }
        success, response = handle_exception(
            requests.get,
            error_code="CFC_1029",
            custom_message=f"Error ocuured while fetching custom classifier with id: '{class_id}'.",
            plugin=self.plugin,
            **params
        )
        if not success:
            raise response
        try:
            parsed_response = handle_status_code(
                response=response,
                error_code="CFC_1050",
                plugin=self.plugin,
                notify=True,
                parse_response=True
            )
            return parsed_response
        except requests.exceptions.HTTPError as error:
            if response.status_code == 404:
                return None
            raise error

    # def all_predefined_classifiers(self):
    #     """Get all predefined classiers will."""
    #     params = {
    #         "url": f"{self.base_url}/api/v2/services/dlp/classifiers/predefined",
    #         "headers": self.headers,
    #         "proxies": self.proxy
    #     }
    #     success, response = handle_exception(
    #         requests.get,
    #         error_code="CFC_1029",
    #         custom_message="Error ocuured while fetching predefined classifiers.",
    #         plugin=self.plugin,
    #         **params
    #     )
    #     if not success:
    #         raise response
    #     parsed_response = handle_status_code(
    #         response=response,
    #         error_code="CFC_1030",
    #         plugin=self.plugin,
    #         notify=True,
    #         parse_response=True,
    #         handle_forbidden=True,
    #     )
    #     return parsed_response

    # def create_overlay_classifier(
    #         self,
    #         class_name: str
    # ):
    #     """Create overlay classifier.

    #     Args:
    #         class_name (str): predefined_classifier.id which will be name of the overlay classifier
    #     """
    #     params = {
    #         "url": f"{self.base_url}/api/v2/services/dlp/classifiers/predefined",
    #         "headers": self.headers,
    #         "proxies": self.proxy,
    #         "data": json.dumps({
    #             "name": class_name
    #         })
    #     }
    #     success, response = handle_exception(
    #         requests.put,
    #         error_code="CFC_1029",
    #         custom_message=f"Error ocuured while updating predefined classifier with name: '{class_name}'.",
    #         plugin=self.plugin,
    #         **params
    #     )
    #     if not success:
    #         raise response
    #     parsed_response = handle_status_code(
    #         response=response,
    #         error_code="CFC_1030",
    #         plugin=self.plugin,
    #         notify=True,
    #         parse_response=True,
    #         handle_forbidden=True,
    #     )
    #     return parsed_response

    # def find_overlay_classifier(
    #         self,
    #         class_name
    # ):
    #     """Find overlay classifier, given predefined classifier id."""
    #     classifiers = self.all_predefined_classifiers()
    #     overlay_classifiers = classifiers["overlayCustomClassifiers"]
    #     overlay_classifier = next(
    #         filter(
    #             lambda classifier: classifier["name"].replace("_overlay_", "") == class_name,
    #             overlay_classifiers
    #         ),
    #         None
    #     )
    #     return overlay_classifier, classifiers["predefinedClassifiers"]["classifiers"]

    # def get_or_create_overlay_classifier(
    #     self,
    #     class_name
    # ):
    #     """Find and if not found than create overlay classifier for a predefined classifier."""
    #     overlay_classifier, predefined_classifiers = self.find_overlay_classifier(class_name=class_name)
    #     if not overlay_classifier:
    #         overlay_classifier = self.create_overlay_classifier(class_name=class_name)
    #     predefined_classifier = next(
    #         filter(
    #             lambda classifier: classifier["id"] == class_name,
    #             predefined_classifiers
    #         )
    #     )
    #     return overlay_classifier, predefined_classifier

    def upload_hash(
            self,
            class_id: str,
            file_path: str,
            ssid: str,
            sessionend: bool,
            negative: bool = False,
    ):
        """Upload CFC hash to the Netskope tenant.

        Args:
            class_id (str): Custom/Overlay classifier id
            file_path (str): Path to the hash file.
            sessionend (bool): Boolean value to set 'True' when the teannt can start training the classifier
            negative (bool, optional): Boolean value True defines the upload hash as a false \
                positive sample for classifier. Defaults to False.
        """
        error_message = "Error occured while uploading CFC hash."
        if not os.path.exists(file_path):
            message = f"{error_message} File does not exist at path {file_path}"
            self.logger.error(
                error_code="CFC_1051",
                details=message,
                message=message
            )
            raise NetskopeClientCFCError(message)
        if not os.path.isfile(file_path):
            message = f"{error_message} {file_path} is not a file."
            self.logger.error(
                error_code="CFC_1052",
                details=message,
                message=message
            )
            raise NetskopeClientCFCError(message)
        if os.path.splitext(file_path)[1].lower() != ".json":
            message = f"{error_message} {file_path} is not a JSON file."
            self.logger.error(
                error_code="CFC_1053",
                details=message,
                message=message
            )
            raise NetskopeClientCFCError(message)
        params = {
            "url": f"{self.base_url}/api/v2/services/dlp/classifiers/custom/{class_id}/hashes",
            "headers": self.headers,
            "proxies": self.proxy,
            "params": {
                "negative": json.dumps(negative),
                "sessionend": json.dumps(sessionend),
                "ssid": ssid,
                "txid": str(uuid4())
            }
        }
        with open(file_path, "rb") as fileobj:
            success, response = handle_exception(
                requests.post,
                error_code="CFC_1054",
                custom_message=f"Error ocuured while uploading CFC hash for classifier id: '{class_id}'",
                plugin=self.plugin,
                **params,
                files={"upload": (os.path.basename(file_path), fileobj)},
            )
        if not success:
            raise response
        parsed_response = handle_status_code(
            response=response,
            error_code="CFC_1055",
            plugin=self.plugin,
            notify=True,
            parse_response=True
        )
        invalid_files = []
        max_limit_reached = False
        if type(parsed_response) in [bytes, bytearray, str]:
            parsed_response = json.loads(parsed_response)

        for file_upload in parsed_response[self.DATA_KEY]:
            if file_upload["status"] == self.MAX_STATUS:
                invalid_files.append(file_upload)
                max_limit_reached = True
            elif file_upload["status"] == self.INVALID_FILE_STATUS:
                invalid_files.append(file_upload)
            elif file_upload["status"] == self.SAME_HASH_STATUS:
                invalid_files.append(file_upload)
        parsed_response["invalid_files"] = invalid_files
        parsed_response["max_limit_reached"] = max_limit_reached
        return parsed_response

    def valid_files_for_classifier(
            self,
            class_id: str
    ):
        """Get valid files uploaded for the classifier.

        Args:
            class_id (str): Custom/Overlay classifier id
        """
        params = {
            "url": f"{self.base_url}/api/v2/services/dlp/classifiers/custom/{class_id}/files",
            "headers": self.headers,
            "proxies": self.proxy,
        }
        success, response = handle_exception(
            requests.get,
            error_code="CFC_1056",
            custom_message=f"Error ocuured while fetching list of valid files for classifier with id: '{class_id}'.",
            plugin=self.plugin,
            **params
        )
        if not success:
            raise response
        parsed_response = handle_status_code(
            response=response,
            error_code="CFC_1057",
            plugin=self.plugin,
            notify=True,
            parse_response=True,
            handle_forbidden=True,
        )
        return parsed_response
