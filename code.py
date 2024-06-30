# Import necessary libraries
import requests
from google.colab import drive
from google.colab import auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io
import os
from urllib.parse import urlparse, unquote

# Mount Google Drive
drive.mount('/content/drive')

# Authenticate and create the Drive API service
auth.authenticate_user()
drive_service = build('drive', 'v3', cache_discovery=False)

def get_filename_from_url(url):
    # Try to get filename from Content-Disposition header
    response = requests.head(url, allow_redirects=True)
    content_disposition = response.headers.get('Content-Disposition')
    if content_disposition:
        import re
        fname = re.findall("filename=(.+)", content_disposition)
        if len(fname) > 0:
            return fname[0].strip('"')

    # Parse URL if Content-Disposition is not available
    path = urlparse(url).path
    filename = os.path.basename(unquote(path))

    # Use a default name if filename is still empty
    if not filename:
        filename = 'downloaded_file'

    return filename

def get_or_create_folder(folder_path):
    # Split the folder path into individual folder names
    folders = folder_path.strip('/').split('/')
    parent_id = 'root'

    for folder in folders:
        # Check if the folder exists
        query = f"name='{folder}' and mimeType='application/vnd.google-apps.folder' and '{parent_id}' in parents and trashed=false"
        results = drive_service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        items = results.get('files', [])

        if not items:
            # Create the folder if it doesn't exist
            file_metadata = {
                'name': folder,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_id]
            }
            folder = drive_service.files().create(body=file_metadata, fields='id').execute()
            parent_id = folder.get('id')
        else:
            # Use the existing folder
            parent_id = items[0]['id']

    return parent_id

def download_to_drive(url, folder_path):
    try:
        # Get filename from URL
        filename = get_filename_from_url(url)
        # Get or create the specified folder
        folder_id = get_or_create_folder(folder_path)

        # Download the file
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Check if the request was successful
        file_content = io.BytesIO()

        total_size = int(response.headers.get('content-length', 0))
        chunk_size = 1024
        for data in response.iter_content(chunk_size):
            file_content.write(data)

        file_content.seek(0)

        # Prepare file metadata
        file_metadata = {'name': filename, 'parents': [folder_id]}

        # Create a MediaIoBaseUpload object
        media = MediaIoBaseUpload(file_content,
                                  mimetype=response.headers.get('content-type'),
                                  resumable=True)

        # Upload the file to Google Drive
        file = drive_service.files().create(body=file_metadata,
                                            media_body=media,
                                            fields='id').execute()

        print(f'File ID: {file.get("id")}')
        print(f'File "{filename}" has been uploaded to Google Drive in the folder: {folder_path}')

    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
url = 'link/to/file'  # Replace with actual download link
folder_path = 'path/to/folder'  # Replace with desired Google Drive folder path

download_to_drive(url, folder_path)

