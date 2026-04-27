"""CFC Hashing Utilities."""
import os
import subprocess
import shutil

from .constants import FILE_PATH, MANUAL_UPLOAD_PATH


PATH_TO_HASHING_SCRIPT = "/opt/ns/bin/nsdlp/dlp-fingerprint20"


def hash_file_folder(
    file_path: str,
    classifier_id,
    classifier_name,
    delete_source: bool = False
) -> str:
    """Hash a file/folder using the CFC hashing script.

    Args:
        file_path (str): The path to the file/folder to hash.

    Returns:
        str: The hash of the file.
    """
    folder_path = os.path.dirname(file_path)
    file_name, _ = os.path.splitext(os.path.basename(file_path))
    output_path = os.path.join(folder_path, file_name + "_hash")
    command = [
        PATH_TO_HASHING_SCRIPT,
        "-f",
        file_path,
        "-i",
        classifier_id,
        "-c",
        classifier_name,
        "-o",
        f"{output_path}/",
        "-e",
        f"{os.path.join(folder_path, file_name + '_tmp')}/",
    ]

    try:
        subprocess.check_call(command)
        if os.path.exists(f"{output_path}/1"):
            shutil.rmtree(f"{output_path}/1")
        if delete_source:
            if os.path.isdir(file_path):
                shutil.rmtree(file_path)
            elif os.path.exists(file_path) and os.path.isfile(file_path):
                os.remove(file_path)
        path_to_hash_file = f"{output_path}/fingerprint20_0_{classifier_id}.json"
        if not os.path.exists(path_to_hash_file):
            raise ValueError(
                "Error occurred while generating CFC hashes."
                f"{path_to_hash_file} does not exist after the hashing."
            )
        return path_to_hash_file
    except subprocess.CalledProcessError as e:
        raise ValueError(f"Error hashing file: {e}")


def base_and_new_path_for_hashing(
    source_config_name,
    destination_config_name,
    classifier_id,
    manual_upload=False,
):
    """Create base and new path for hashing."""
    base_path = FILE_PATH
    if manual_upload:
        base_path = MANUAL_UPLOAD_PATH
    files_base_path = f"{base_path}/{source_config_name}"
    new_path = (
        f"{base_path}/{destination_config_name}/"
        f"{source_config_name}/{classifier_id}"
    )
    if manual_upload:
        new_path = (
            f"{base_path}/{destination_config_name}/_manual_upload/"
            f"{source_config_name}/{classifier_id}"
        )
    return files_base_path, new_path


def current_and_new_file_paths(
    file,
    files_base_path,
    new_path,
    manual_upload=False,
):
    """Prepare current and new file paths."""
    if manual_upload:
        current_file_path = files_base_path
        current_file_new_path = new_path
        if file["path"]:
            current_file_path += (
                f"/{os.path.dirname(file['path']).replace('/', '_')}"
            )
            current_file_new_path += (
                f"/{os.path.dirname(file['path']).replace('/', '_')}"
            )
        current_file_path += f"/{file['file']}"
    else:
        current_file_path = (
            f"{files_base_path}/{file['dirUuid']}/"
            f"{file['file']}"
        )
        current_file_new_path = f"{new_path}/{file['dirUuid']}"
    return current_file_path, current_file_new_path


def create_hashes(
    source_config_name,
    destination_config_name,
    classifier_name,
    classifier_id,
    files,
    manual_upload=False,
):
    """Create hashes."""
    files_base_path, new_path = base_and_new_path_for_hashing(
        source_config_name,
        destination_config_name,
        classifier_id,
        manual_upload=manual_upload,
    )
    if not os.path.exists(new_path):
        os.makedirs(new_path)
    new_file_path_mappings = {}
    for file in files:
        current_file_path, current_file_new_path = current_and_new_file_paths(
            file,
            files_base_path,
            new_path,
            manual_upload=manual_upload,
        )
        if not os.path.exists(current_file_new_path):
            os.makedirs(current_file_new_path)
        if file["file"] and os.path.exists(current_file_path):
            with open(f"{current_file_new_path}/{file['file']}", "wb") as f:
                shutil.copyfileobj(open(current_file_path, "rb"), f)
        new_file_path_mappings[f"{current_file_new_path}/{file['file']}"] = file
    path_to_hash = hash_file_folder(
        new_path, classifier_id, classifier_name, delete_source=True
    )
    return path_to_hash, new_file_path_mappings
