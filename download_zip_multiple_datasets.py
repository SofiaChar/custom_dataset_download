import requests
import os
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import datetime, timedelta
import shutil

token = os.environ.get('VH_TOKEN')
HEADERS = {'Authorization': 'Token %s' % token}

# Retry settings
retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])


def get_available_filename(original_name):
    """Modify the file name if the file already exists by appending a numeric suffix (_1, _2, etc.)."""
    base_name, extension = os.path.splitext(original_name)
    counter = 1
    while os.path.exists(original_name):
        original_name = f"{base_name}_{counter}{extension}"
        counter += 1
    return original_name


def fetch_dataset_version_details(dataset_version_id):
    """Fetch dataset version details from the API."""
    url = f'https://app.valohai.com/api/v0/dataset-versions/{dataset_version_id}/'
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 429:  # Rate limit exceeded
        print("Rate limit exceeded. Waiting for 5 seconds before retrying...")
        time.sleep(5)
        return fetch_dataset_version_details(dataset_version_id)
    if response.status_code != 200:
        print(f"Failed to fetch dataset version info. HTTP status code: {response.status_code}")
        return None
    return response.json()


def download_and_save_file(datum_id, file_name, directory):
    """Download and save the file from the provided datum ID and desired file name."""
    download_url = f'https://app.valohai.com/api/v0/data/{datum_id}/download/'
    response = requests.get(download_url, headers=HEADERS)
    if response.status_code == 429:  # Rate limit exceeded
        print("Rate limit exceeded. Waiting for 5 seconds before retrying...")
        time.sleep(5)
        return download_and_save_file(datum_id, file_name, directory)
    if response.status_code != 200:
        print(f"Failed to retrieve download URL. HTTP status code: {response.status_code}")
        return

    sess = requests.Session()
    sess.mount('https://', HTTPAdapter(max_retries=retries))
    download_response = sess.get(response.json()['url'])
    if download_response.status_code == 200:
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
        filename = get_available_filename(file_name)
        file_path = os.path.join(directory, filename)
        with open(file_path, 'wb') as my_file:
            my_file.write(download_response.content)
        print(f"File downloaded successfully and saved as {filename}")
    else:
        print(f"Failed to download the file. HTTP status code: {download_response.status_code}")


def handle_files(files, directory):
    """Process each file in the dataset version details."""
    for file in files:
        datum_id = file['datum']['id']
        file_name = file['datum']['name']
        download_and_save_file(datum_id, file_name, directory)


def fetch_and_handle_version(version, ds_path):
    """Fetch dataset version details and handle files."""
    dataset_details = fetch_dataset_version_details(version["id"])
    if dataset_details:
        handle_files(dataset_details['files'], os.path.join(ds_path, version["name"]))


def get_dataset(dataset_id, dataset_name, start_version):
    url = f'https://app.valohai.com/api/v0/dataset-versions/?dataset={dataset_id}'
    sess = requests.Session()
    sess.mount('https://', HTTPAdapter(max_retries=retries))

    dataset_directory = os.path.join(os.getcwd(), dataset_name)
    if not os.path.exists(dataset_directory):
        os.makedirs(dataset_directory)

    start_processing = False

    while url:
        response = sess.get(url, headers=HEADERS)
        if response.status_code == 429:  # Rate limit exceeded
            print("Rate limit exceeded. Waiting for 5 seconds before retrying...")
            time.sleep(5)
            continue
        if response.status_code != 200:
            print(f"Failed to fetch dataset version info. HTTP status code: {response.status_code}")
            print(response.text)
            return None
        data = response.json()
        dataset_versions = data["results"]

        # Process each version immediately after fetching it
        for version in dataset_versions:
            if version['name'] == start_version:
                start_processing = True
                print('start_processing TRUE')
            if start_processing:
                print(f"Fetching version {version}")
                fetch_and_handle_version(version, dataset_directory)

        url = data.get("next")  # Get the URL for the next page of results


def zip_directory(directory_path, output_path):
    """Zip the contents of the directory."""
    shutil.make_archive(output_path, 'zip', directory_path)
    print(f"Zipped {directory_path} to {output_path}.zip")

if __name__ == '__main__':
    base_start_date = datetime.now() - timedelta(days=7)
    base_date_str = base_start_date.strftime('%Y-%m-%d')
    # Dataset IDs with corresponding hardcoded times
    datasets = [
        ('pjm-rtm-forecast-0', '018e52a3-541b-845a-40b9-7224ce0a66b2', '09-05'),
        ('pjm-rtm-forecast-1', '018e52a3-3f76-354c-43fb-c329adcbe154', '09-05'),
        ('pjm-rtm-forecast-17to466', '018e52a3-2206-25d2-1275-e460d41ec6d4', '09-05'),
        ('pjm-rtm-forecast-2to16', '018e52a2-ecaa-1f6b-284c-7e50ae7c39fc', '09-15')
    ]

    for dataset_name, dataset_id, time_str in datasets:
        # Combine base date with hardcoded time for the dataset
        start_date_str = f"{base_date_str}-{time_str}"
        print(f"Processing dataset {dataset_name} with ID {dataset_id} starting from {start_date_str}")

        # Fetch and process the dataset
        get_dataset(dataset_id, dataset_name, start_date_str)

        # Create zip of the dataset directory
        output_zip_name = f'/valohai/outputs/{dataset_name}_{start_date_str}'

        zip_directory(dataset_name, output_zip_name)