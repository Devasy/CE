# flake8: noqa
"""
   :synopsis: This is a sample program that illustrates how
        Netskope Edm Csv File Upload Rest Apis works.
   :copyright: (c) 2024 Netskope, Inc. All rights reserved.
"""
import os
import tarfile
import json
import hashlib
import fnmatch
import uuid
from pathlib import Path
import requests
import argparse
from netskope.common.utils.logger import Logger
from netskope.common.utils import add_user_agent
logger = Logger()


class DlpEdmStagingApi:
    """
    The DlpEdmStagingApi class provides an interface for interacting with the DLP EDM Staging API.
    It allows creating, uploading, completing, aborting, and applying EDM file uploads.

    The constructor initializes the API with the provided authentication token,
    server URL, and port number.

    The class provides the following methods:

    - create_edm_upload(edm_filename, tgz_filename): Creates an EDM file upload session by
        sending a POST request to the DLP EDM Staging API with the provided EDM filename,
        file size, and SHA1 hash.
    - upload_edm_part(fileid, uploadid, partnum, data): Uploads a part of an EDM file to
        the specified upload ID.
    - complete_edm_upload(fileid, uploadid, parts): Completes an EDM file upload by sending
        a POST request to the specified URL with the provided file ID, upload ID, and part list.
    - abort_edm_upload(fileid): Aborts an EDM file upload by sending a DELETE request to
        the specified URL with the provided file ID.
    - apply_edm_upload(fileid): Applies an EDM file to a specified file ID.
    - status_edm_upload(fileid, uploadid): Retrieves the status of an EDM file upload.
    """

    def __init__(self, auth_token, server, port):
        """
        Constructor for the DLP EDM Staging API.

        Initializes the API with the provided authentication token,
        server URL, and port.

        Args:
            auth_token (str): The authentication token.
            server (str): The server URL.
            port (int): The port number.

        Returns:
            None
        """
        self.auth_token = auth_token
        self.server = server
        self.port = port


    def _get_url_prefix(self):
        """
        Get the URL prefix for the DLP EDM Staging API.

        Returns:
            str: The URL prefix.
        """
        if self.port > 0:
            return f'https://{self.server}:{self.port}/api/v2/services/dlp/edm/file'

        return f'https://{self.server}/api/v2/services/dlp/edm/file'


    def _get_headers(self):
        return add_user_agent({
            "Content-Type": "application/json",
            "netskope-api-token": self.auth_token
        })


    def _get_file_hash(self, filename):
        """
        Get the file hash for the given filename.

        Args:
            filename (str): The filename to get the hash for.

        Returns:
            str: The file hash.
        """
        hasher = hashlib.sha1()
        rbufsize = 1024 * 1024

        with open(filename, 'rb') as fp:
            chunk = fp.read(rbufsize)
            while chunk != b'':
                hasher.update(chunk)
                chunk = fp.read(rbufsize)

        return hasher.hexdigest()


    def create(self, edm_filename, tgz_filename, keep_staging=False, description=None):
        """
        Creates an EDM file upload session by sending a POST request to the DLP EDM Staging
        API with the provided EDM filename, file size, and SHA1 hash.

        Args:
            edm_filename (str): The filename of the EDM file being uploaded.
            tgz_filename (str): The filename of the compressed EDM file.
            description (str): Optional description of the compressed EDM file.

        Returns:
            tuple: A tuple containing the HTTP status code, the JSON response data, and the response message.
        """
        # curl -vvv -XPOST -H "Content-Type: application/json" -H "netskope-api-token: 123abc"
        #   -d "{\"edm_filename\":\"test\", \"sha1\": \"111111111\", \"size\": 100}"
        #   "https://<tenant url>/api/v2/services/dlp/edm/file/staging"
        # returns: {'fileid': 'test_upload.csv', 'msg': 'good', 'status': 'success', 'uploadid': 'MDE2....'}

        url = self._get_url_prefix() + '/staging'
        hdr = self._get_headers()
        json_data = {}
        message = ""

        fsize = os.path.getsize(tgz_filename)
        fsha1 = self._get_file_hash(tgz_filename)
        payload = {"edm_filename": edm_filename, "tgz_filename": tgz_filename, "sha1": fsha1, "size": fsize}

        if description:
            payload['description'] = description

        if keep_staging:
            payload['keep_staging'] = True

        response = requests.post(url, headers=hdr, data=json.dumps(payload))

        # Get the status code and message from the response
        status_code = response.status_code
        if status_code <= 300:
            # if there is json response
            try:
                if response.json():
                    json_data = response.json()
            except ValueError:
                # Response is not valid JSON
                json_data = {}
        else:
            message = response.text

        # return JSON response
        return status_code, json_data, message


    def upload_part(self, fileid, uploadid, partnum, data):
        """
        Uploads a part of an EDM file to the specified upload ID.

        Args:
            fileid (str): The ID of the file being uploaded.
            uploadid (str): The ID of the upload session.
            partnum (int): The part number of the file being uploaded.
            data (bytes): The data to be uploaded.

        Returns:
            tuple: A tuple containing the HTTP status code, the JSON response data, and the response message.
        """
        # curl -vvv -XPUT -H "Content-Type: application/octet-stream" -H "netskope-api-token: 123abc" -d "test manual upload"
        #   "https://<tenant url>/api/v2/services/dlp/edm/file/staging/<fileid>?part=1&size=10&uploadid=xxxx"
        # returns: {"etag":"\"b26dc8fc94ff158870d52408b2004462\"","msg":"good","partnumber":1}

        url = self._get_url_prefix() + f"/staging/{fileid}?part={partnum}&size={len(data)}&uploadid={uploadid}"
        hdr = self._get_headers()
        hdr['Content-Type'] = 'application/octet-stream'
        json_data = {}
        message = ""

        response = requests.put(url, headers=hdr, data=data)


        # Get the status code and message from the response
        status_code = response.status_code
        if status_code <= 300:
            # if there is json response
            try:
                if response.json():
                    json_data = response.json()
            except ValueError:
                # Response is not valid JSON
                json_data = {}
        else:
            message = response.text

        # return JSON response
        return status_code, json_data, message


    def complete(self, fileid, uploadid, parts):
        """
        Completes an EDM file upload by sending a POST request to the specified URL
            with the provided file ID, upload ID, and part list.

        Args:
            fileid (str): The ID of the file being uploaded.
            uploadid (str): The ID of the upload.
            parts (list): A list of part IDs that have been uploaded.

        Returns:
            tuple: A tuple containing the HTTP status code, the JSON response data, and the response message.
        """
        # curl -vvv -XPOST -H "Content-Type: application/json" -H "netskope-api-token: 123abc"
        #   -d "{\"partlist\": [\"b26dc8fc94ff158870d52408b2004462\"]}"
        #   "https://<tenant url>/api/v2/services/dlp/edm/file/staging/<fileid>?uploadid=MDE..."

        url = self._get_url_prefix() + f"/staging/{fileid}?uploadid={uploadid}"
        hdr = self._get_headers()
        payload = {"partlist": parts}
        json_data = {}
        message = ""

        print(f"post completed url: {url}, parts: {payload}")
        response = requests.post(url, headers=hdr, data=json.dumps(payload))

        # Get the status code and message from the response
        status_code = response.status_code
        if status_code <= 300:
            # if there is json response
            try:
                if response.json():
                    json_data = response.json()
            except ValueError:
                # Response is not valid JSON
                json_data = {}
        else:
            message = response.text

        # return JSON response
        return status_code, json_data, message


    def abort(self, fileid):
        """
        Aborts an EDM file upload by sending a DELETE request to the specified URL with the provided file ID.

        Args:
            fileid (str): The ID of the file being uploaded.

        Returns:
            tuple: A tuple containing the HTTP status code, and the response message.
        """
        #curl -vvv -XDELETE -H "Content-Type: application/json" -H "netskope-api-token: 123abc"
        #   -d "{\"partlist\": [\"b26dc8fc94ff158870d52408b2004462\"]}"
        #   "https://<tenant url>/api/v2/services/dlp/edm/file/staging/<fileid>"
        url = self._get_url_prefix() + f"/staging/{fileid}"
        hdr = self._get_headers()
        json_data = {}

        response = requests.delete(url, headers=hdr)

        # Get the status code and message from the response
        status_code = response.status_code
        if status_code < 300:
            # Success has no response
            return status_code, "Success"

        message = response.text

        # return JSON response
        return status_code, message


    def apply(self, fileid):
        """
        Applies an EDM (Exact Data Match) file to a specified file ID.

        Args:
            fileid (str): The ID of the file to apply the EDM file to.

        Returns:
            tuple: A tuple containing the HTTP status code, a boolean indicating whether
                the operation was successful, and the response message.
        """
        #curl -vvv -XPOST -H "Content-Type: application/json" -H "netskope-api-token: 123abc"
        #   "https://<tenant url>/api/v2/services/dlp/edm/file/apply/<file_id>"
        url = self._get_url_prefix() + f'/apply/{fileid}'
        hdr = self._get_headers()
        status = False
        message = ""

        response = requests.post(url, headers=hdr)

        # Get the status code and message from the response
        status_code = response.status_code

        # If the request was successful (i.e., returned a status code between 200 and 299), parse the JSON data from the response
        if status_code >= 200 and status_code < 300:
            status = True
        else:
            message = response.text

        # return status
        return status_code, status, message


    def get_status(self, fileid):
        """
        Retrieves the status of an EDM (Exact Data Match) file upload.

        Args:
            fileid (str): The ID of the file to check the upload status for.

        Returns:
            tuple: A tuple containing the HTTP status code, the status code, and the response message.
        """
        #curl -vvv -XGET -H "Content-Type: application/json" -H "netskope-api-token: 123abc"
        #   "https://<tenant url>/api/v2/services/dlp/edm/file/staging/<fileid>"
        url = self._get_url_prefix() + f"/staging/{fileid}"
        hdr = self._get_headers()
        json_data = {}
        message = ""

        response = requests.get(url, headers=hdr)

        # Get the status code and message from the response
        status_code = response.status_code
        if status_code == 204:
            # No status found.  That is the case when a staging file has been applied successfully.
            return status_code, None, "No status found"

        if status_code <= 300:
            # if there is json response
            try:
                if response.json():
                    json_data = response.json()
            except ValueError:
                # Response is not valid JSON
                json_data = {}
        else:
            message = response.text

        # return status
        return status_code, json_data, message


    def list_files(self):
        """
        Retrieves a list of EDM (Exact Data Match) files that have been uploaded.

        Args:
            fileid (str): The ID of the file to check the upload status for.
            uploadid (str): The ID of the upload to check the status for.

        Returns:
            tuple: A tuple containing the HTTP status code,
                the JSON data from the response, and the response message.
        """
        #curl -vvv -XGET -H "Content-Type: application/json" -H "netskope-api-token: 123abc"
        #   "https://<tenant url>/api/v2/services/dlp/edm/file/staging/list"
        url = self._get_url_prefix() + f"/staging/list"
        hdr = self._get_headers()
        json_data = {}
        message = ""

        response = requests.get(url, headers=hdr)

        # Get the status code and message from the response
        status_code = response.status_code
        if status_code <= 300:
            # if there is json response
            try:
                if response.json():
                    json_data = response.json()
            except ValueError:
                # Response is not valid JSON
                json_data = {}
        else:
            message = response.text

        # return status
        return status_code, json_data, message



class StagingManager:

    def __init__(self):
        """
        Initializes the StagingManager class with default values for the authentication token, server, port, and verbosity.
        """
        self.auth_token = ""
        self.server = ""
        self.port = 0
        self.verbose = False
        self.description = ""
        self.keep_staging_file = False
        self.client = None

    def _is_valid_tgz(self, file_path):
        """
        Check if a file is a valid .tgz (tar.gz) archive.

        Args:
            file_path (str): Path to the file.

        Returns:
            bool: True if the file is a valid .tgz archive, False otherwise.
        """
        try:
            with tarfile.open(file_path, 'r:gz') as tar:
                # Check if the archive is valid by iterating over its members
                tar.getmembers()
                return True
        except tarfile.ReadError:
            print(f"Error: {file_path} is not a valid .tgz archive")
        except Exception as e:
            print(f"Error: {e}")

        return False


    def _get_metadata_filename(self, file_path):
        """
        Extracts the EDM CSV file name from a .tgz (tar.gz) archive.

        Args:
            file_path (str): Path to the .tgz archive.

        Returns:
            str: The name of the EDM CSV file within the .tgz archive, or an empty string if the file is not found or the archive is invalid.
        """
        edm_csv_fname = ""
        try:
            with tarfile.open(file_path, 'r:gz') as tar:
                # get all file names
                names = tar.getnames()

                # Check if the archive is valid by iterating over its members
                for name in names:
                    if fnmatch.fnmatch(name, "*/pdd_metadata_*.json") or fnmatch.fnmatch(name, "pdd_metadata_*.json"):
                        reader = tar.extractfile(name)
                        pdd_metadata = json.loads(reader.read().decode('utf-8'))
                        edm_csv_fname = pdd_metadata['filename']

                print(f"edm csv file name: {edm_csv_fname}")
                return edm_csv_fname
        except tarfile.ReadError:
            print(f"Error: {file_path} is not a valid .tgz archive")
        except Exception as e:
            print(f"Error: {e}")

        return edm_csv_fname


    def load_auth_file(self, auth_file):
        """
        Loads the authentication token from an external file.

        Args:
            auth_file (str): Path to the authentication token file.

        Returns:
            None
        """
        with open(auth_file) as fp:
            token = fp.read()
            token = token.strip()
            self.set_auth_token(token)


    def set_auth_token(self, auth_token):
        self.auth_token = auth_token

    def get_auth_token(self):
        return self.auth_token

    def set_server(self, server):
        self.server = server

    def get_server(self):
        return self.server

    def set_port(self, port):
        self.port = port

    def get_port(self):
        return self.port

    def set_description(self, description):
        self.description = description

    def set_keep_staging_file(self, keep):
        self.keep_staging_file = keep

    def get_description(self):
        return self.description

    def set_verbose(self, verbose):
        self.verbose = verbose

    def is_verbose(self):
        return self.verbose

    def load_client(self):
        self.client = DlpEdmStagingApi(self.auth_token, self.server, self.port)



    def list(self):
        """
        Lists the staging files.

        Returns:
            None
        """
        status_code, resp, msg = self.client.list_files()
        if status_code > 300:
            print(f"List staging file error")
            if resp:
                print(f"Error response: {resp}")
            elif msg:
                print(f"Error response: {msg}")
            return

        print(f"List staging files: {resp}")


    def status(self, file_id):
        """
        Retrieves the status of a staging file.

        Args:
            file_id (str): The ID of the staging file to retrieve the status for.

        Returns:
            None
        """
        if not file_id:
            return False, "Missing file id", {}

        status_code, resp, msg = self.client.get_status(file_id)
        if status_code > 300:
            return False, f"Error response: {resp}" if resp else f"Error response: {msg}", {}

        print(f"Get staging file {file_id} status succeeded")

        print(f"Status: {json.dumps(resp, indent=4)}")
        return True, msg, resp


    def upload(self, filename):
        """
        Uploads an EDM (Exact Data Match) file to the staging area.
        This method performs the following steps:
        1. Checks if the provided file is a valid .tgz (tar.gz) file.
        2. Extracts the EDM CSV filename from the .tgz file.
        3. Creates a new staging file on the server using the EDM CSV filename.
        4. Uploads the .tgz file to the server in parts, with the maximum part size specified by the server.
        5. Completes the staging process by providing the list of uploaded part ETags.
        6. Automatically applies the staged EDM file.

        Args:
            filename (str): The path to the EDM file to upload.

        Returns:
            status, message, response
        """
        if not self._is_valid_tgz(filename):
            return False, f"{filename} is not a tgz file", {}

        # get edm csv filename
        edm_meta_filename = self._get_metadata_filename(filename)

        status_code, resp, msg = self.client.create(edm_meta_filename,
                                                    filename,
                                                    self.keep_staging_file,
                                                    self.description)

        if status_code > 300:
            return False, f"Error response: {resp}" if resp else f"Error response: {msg}", {
                "status_code": status_code,
                "message": msg,
                "response": resp
            }


        uploadid = resp.get('uploadid')
        fileid = resp.get('fileid')
        max_part_size = resp.get('part_max_size', 0)

        # check if all return values are correct
        if max_part_size < 1:
            return False, f"Invalid max part size: {max_part_size}", {}

        if not uploadid:
            return False, "No upload id", {}

        if not fileid:
            return False, "No file id", {}

        if self.verbose:
            logger.info(f"Started staging file - File ID: {fileid}; Upload ID: {uploadid}")

        partnum = 1
        parts = {}
        with open(filename, 'rb') as fp:
            while True:
                payload = fp.read(max_part_size)
                if not payload:
                    break
                status_code, resp, msg = self.client.upload_part(fileid, uploadid, partnum, payload)

                if status_code > 300:
                    return False, f"Error response: {resp}" if resp else f"Error response: {msg}", {
                        "status_code": status_code,
                        "message": msg,
                        "response": resp
                    }

                # get the result etag
                parts[partnum] = resp['etag']
                partnum += 1

        # sort parts by key and return list of values
        sorted_parts = sorted(parts.items())
        etags = [value for _, value in sorted_parts]

        # if verbose, print all of the etags
        if self.verbose:
            logger.info(f"All Etags (total: {len(etags)}):\n {json.dumps(etags, indent=4)}")

        # completed the staging process
        status_code, resp, msg = self.client.complete(fileid, uploadid, etags)

        if status_code > 300:
            return False, f"Error response: {resp}" if resp else f"Error response: {msg}", {
                "status_code": status_code,
                "message": msg,
                "response": resp
            }

        logger.info(f"Upload EDM tgz file {filename} completed, status: {resp}")
        # apply automatically
        apply_status, apply_msg, context = self.apply(fileid)
        return True, apply_msg, {
            "file_id": fileid,
            "upload_id": uploadid,
            "apply_status": apply_status,
            "status_code": context.get("status_code"),
        }


    def delete(self, file_id):
        """
        Deletes a staging file upload.

        Args:
            file_id (str): The ID of the staging file to delete.

        Returns:
            None
        """
        if not file_id:
            return False, "Missing file id while deleting Edm staging file", {}

        status_code, msg = self.client.abort(file_id)
        if status_code > 300:
            return False, f"Error response while deleting Edm staging file: {msg}", {
                "status_code": status_code,
                "message": msg,
            }

        return True, f"Delete Edm staging file {file_id} succeeded", {
            "status_code": status_code,
            "message": msg,
        }


    def apply(self, file_id):
        """
        Applies a staging file to an EDM filename.

        Args:
            file_id (str): The ID of the staging file to apply.

        Returns:
            None
        """
        if not file_id:
            return False, "Missing file id while applying", {}

        status_code, resp, msg = self.client.apply(file_id)
        if status_code > 300:
            return False, f"Error response while applying Edm staging file: {resp}" if resp else f"Error response while applying Edm staging file: {msg}", {
                "status_code": status_code,
                "message": msg,
                "response": resp
            }

        if not resp:
            return False, f"Error response while applying Edm staging file: {resp}" if resp else f"Error response while applying Edm staging file: {msg}", {
                "status_code": status_code,
                "message": msg,
                "response": resp
            }

        # print(f"Apply Edm staging file {file_id} succeeded")
        return True, f"Apply Edm staging file {file_id} succeeded", {
            "status_code": status_code,
            "message": msg,
            "response": resp
        }



def main():
    staging_manager = StagingManager()

    parser = argparse.ArgumentParser(description='Netskope Dlp Edm Upload Client')
    # set global arguments
    parser.add_argument('-s', '--server', type=str, required=True, help='Server URL')
    parser.add_argument('-p', '--port', type=int, default=0, help='Port number to connect to')
    parser.add_argument('-c', '--credential', type=str, required=True, help='File that stores the authentication credential')
    parser.add_argument('-v', '--verbose', action='store_true', help='Print more verbose output')

    subparsers = parser.add_subparsers(dest='command')

    list_parser = subparsers.add_parser('list', help='List all existing staging file IDs')
    list_parser.set_defaults(func=lambda: staging_manager.list())

    status_parser = subparsers.add_parser('status', help='Status with staging file ID and Upload ID as arguments')
    status_parser.add_argument('file_id', type=str, help='Staging file ID to show status for')
    status_parser.set_defaults(func=lambda args: staging_manager.status(args.file_id))

    upload_parser = subparsers.add_parser('upload', help='Upload a staging file with filename')
    upload_parser.add_argument('-d', '--description', type=str, help='Description of this upload')
    upload_parser.add_argument('-k', '--keep', action='store_true', help='Do not remove staging file after apply succeeded')
    upload_parser.add_argument('filename', type=str, help='Edm output .tgz file to upload')
    upload_parser.set_defaults(func=lambda args: staging_manager.upload(args.filename))

    delete_parser = subparsers.add_parser('delete', help='Delete with staging file ID as argument')
    delete_parser.add_argument('file_id', type=str, help='Staging file ID to delete')
    delete_parser.set_defaults(func=lambda args: staging_manager.delete(args.file_id))

    apply_parser = subparsers.add_parser('apply', help='Apply with staging file ID and edm_filename as arguments')
    apply_parser.add_argument('file_id', type=str, help='Staging file ID to apply')
    apply_parser.set_defaults(func=lambda args: staging_manager.apply(args.file_id))

    args = parser.parse_args()
    if hasattr(args, 'func'):
        staging_manager.load_auth_file(args.credential)
        staging_manager.set_server(args.server)
        staging_manager.set_port(args.port)
        staging_manager.set_verbose(args.verbose)
        staging_manager.load_client()

        if args.command == "upload":
            if args.description:
                staging_manager.set_description(args.description)

            if args.keep:
                staging_manager.set_keep_staging_file(args.keep)

        if args.command == "list":
            args.func()
        else:
            args.func(args)
