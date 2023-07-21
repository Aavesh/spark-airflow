import requests
import os
import zipfile

def download_kaggle_data(url, output_path):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(output_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        print(f"Data downloaded successfully to {output_path}")
    except requests.exceptions.RequestException as e:
        print(f"Error occurred during data download: {e}")

def unzip_data(zip_file, output_dir):
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
        print(f"Data unzipped successfully to {output_dir}")
    except zipfile.BadZipFile as e:
        print(f"Error occurred during unzipping: {e}")
    except Exception as e:
        print(f"An error occurred during unzipping: {e}")

if __name__ == "__main__":
    kaggle_url = "https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset/download?datasetVersionNumber=2"
    output_file_path = "/path/to/your/output_directory/stock_market_dataset.zip"
    output_extracted_dir = "/path/to/your/output_directory/"

    download_kaggle_data(kaggle_url, output_file_path)
    unzip_data(output_file_path, output_extracted_dir)
