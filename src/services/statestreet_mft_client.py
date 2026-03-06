import requests
import urllib3
import os
import logging
from pathlib import Path
from config import MFT_URL, MFT_USERNAME, MFT_PASSWORD, MFT_CERT_PATH, MFT_KEY_PATH

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LOCAL_DOWNLOAD_FOLDER = Path(__file__).parent.parent.parent / "downloads"
FOLDERS = [
    "/ETFGlobalHarvest/fromSSC",
    "/Harvestportf"  # add or remove folders as needed
]


def create_session():
    session = requests.Session()
    session.cert = (MFT_CERT_PATH, MFT_KEY_PATH)
    return session


def login(session):
    response = session.post(
        f"{MFT_URL}/auth/login",
        data={"username": MFT_USERNAME, "password": MFT_PASSWORD},
        timeout=10,
        verify=False
    )
    if response.status_code == 200:
        logger.info("Login successful")
    else:
        raise Exception(f"Login failed: {response.status_code}")
    return session


def list_files(session, folder_path):
    response = session.get(
        f"{MFT_URL}/files{folder_path}",
        params={
            "spcmd": "splist",
            "sort": "filename",
            "direction": "ASC",
            "page": 0,
            "start": 0,
            "limit": 100,
            "correlationId": ""
        },
        verify=False,
        timeout=10
    )
    response.raise_for_status()
    data = response.json()
    files = data.get("files", [])
    logger.info(f"Found {len(files)} items in {folder_path}")
    return files

def run():
    session = create_session()
    login(session)

    for folder in FOLDERS:
        logger.info(f"Processing folder: {folder}")
        files = list_files(session, folder)
        print("Raw files:", files)  # temporary

        for file in files:
            if not file.get("directory"):  # skip folders, only download files
                filename = file.get("filename")
                download_file(session, folder, filename)

    logger.info("All done!")


def download_file(session, folder_path, filename):
    """
    Downloads a single file from the MFT server and saves it locally.
    """
    LOCAL_DOWNLOAD_FOLDER.mkdir(parents=True, exist_ok=True)
    local_path = LOCAL_DOWNLOAD_FOLDER / filename

    response = session.get(
        f"{MFT_URL}/files{folder_path}/{filename}",  # path is part of the URL, no params needed
        verify=False,
        timeout=30,
        stream=True
    )

    response.raise_for_status()

    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info(f"Downloaded: {filename}")
    return local_path


if __name__ == "__main__":
    run()