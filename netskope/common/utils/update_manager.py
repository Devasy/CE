"""Update manager class."""

import os
import re
import traceback
from threading import Thread
from typing import Optional
import requests

from .singleton import Singleton
from .logger import Logger
from netskope.common.utils.handle_exception import (
    handle_exception,
    handle_status_code,
)
from ..api import __version__


class UpdateException(Exception):
    """Custom update exception."""

    pass


class UpdateManager(metaclass=Singleton):
    """Update manager class."""

    def __init__(self):
        """Initialize update manager."""
        self.logger = Logger()

    def get_changelog_from_image(self, tag):
        """Get the changelog from given image tag.

        Args:
            tag (str): image tag

        Returns:
            str: Returns changelog from given image tag if new version is available.
        """
        repo, tag_name = tag.split(":")
        image_url = f"https://hub.docker.com/v2/repositories/{repo}/tags/{tag_name}/images"
        headers = {}
        success, response = handle_exception(
            method=requests.get,
            error_code="CE_1101",
            custom_message="Error occurred while getting changelog.",
            headers=headers,
            url=image_url,
        )
        if not success:
            raise response
        else:
            response = handle_status_code(
                response,
                error_code="CE_1102",
                custom_message="Error occurred while getting changelog."
            )
            image_ce_version, changelog = None, None
            if response:
                response = response[0]
                for image_layer in reversed(response.get("layers", [])):
                    instruction = image_layer.get("instruction", {})
                    if "LABEL com.netskope.ce_version" in instruction:
                        image_ce_version = (
                            re.findall(r"\d.+", instruction)[0]
                            if re.findall(r"\d.+", instruction)
                            else None
                        )
                    if "LABEL com.netskope.ce.changelog" in instruction:
                        changelog = instruction.strip().replace(
                            "LABEL com.netskope.ce.changelog=", ""
                        )
                    if changelog and image_ce_version:
                        if str(image_ce_version) > str(__version__):
                            return changelog
                        else:
                            self.logger.debug(
                                f"No updates available for tag: {tag}"
                            )
                            break
            else:
                self.logger.debug(f"Information is not available for tag: {tag}")
            return None

    def get_changelog(self, keyword) -> Optional[str]:
        """Get changelog of the new image if update is available."""
        try:
            tag = None
            if keyword == "core":
                tag = os.environ.get("CORE_LATEST_VERSION_TAG", None)
            if keyword == "ui":
                tag = os.environ.get("UI_LATEST_VERSION_TAG", None)
            if tag:
                self.logger.debug(f"Checking updates for {keyword} container.")
                return self.get_changelog_from_image(tag)
            else:
                self.logger.debug(
                    "Error occurred while fetching the changelog of the new image. "
                    "Not able to fetch image details of CORE and UI."
                )
        except Exception as e:
            self.logger.error(
                f"Error occurred while checking updates for container {e}",
                details=traceback.format_exc(),
                error_code="CE_1020",
            )
        return None

    def update(self):
        """Make an HTTP call to watchtower to update containers."""
        try:
            self.logger.debug("Container update has been triggered.")
            Thread(
                target=lambda: requests.get(
                    "http://watchtower:8080/v1/update",
                    headers={
                        "Authorization": f'Bearer {os.environ.get("WATCHTOWER_HTTP_API_TOKEN")}'
                    },
                )
            ).start()
        except Exception as ex:
            self.logger.error(
                "Error occurred while updating the containers.",
                details=repr(ex),
                error_code="CE_1021",
            )
            raise UpdateException(
                "Error occurred while updating the containers."
            )
