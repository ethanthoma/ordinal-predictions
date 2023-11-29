from dataclasses import dataclass, fields
import configparser

from google.cloud import bigquery
from google.cloud import storage
import os
import pandas as pd
import random
from typing import List

import warnings
warnings.filterwarnings(
    "ignore", 
    "Your application has authenticated using end user credentials"
)


@dataclass
class DataFetchedParams:
    local_file_name:    str

    cache_dir:          str = None


    def __post_init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')

        for field in fields(self):
            if getattr(self, field.name) is None:
                section = field.name
                if section not in config:
                    section = __name__

                setattr(self, field.name, config.get(section, field.name))


def data_fetched(params: DataFetchedParams):
    combined_df_filepath = os.path.join(
        params.cache_dir,
        params.local_file_name
    )

    return os.path.exists(combined_df_filepath)


@dataclass
class LoadFetchedDataParams:
    local_file_name:    str

    cache_dir:          str = None


    def __post_init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')

        for field in fields(self):
            if getattr(self, field.name) is None:
                section = field.name
                if section not in config:
                    section = __name__

                setattr(self, field.name, config.get(section, field.name))


def load_fetched_data(params: LoadFetchedDataParams):
    combined_df_filepath = os.path.join(
        params.cache_dir,
        params.local_file_name
    )

    if os.path.exists(combined_df_filepath):
        return pd.read_csv(combined_df_filepath)


@dataclass
class FetchFromTableParams:
    table_id:           str
    local_file_name:    str

    cache_dir:          str = None
    bucket_name:        str = None


    def __post_init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')

        for field in fields(self):
            if getattr(self, field.name) is None:
                section = field.name
                if section not in config:
                    section = __name__

                setattr(self, field.name, config.get(section, field.name))


def fetch_from_table(params: FetchFromTableParams) -> pd.DataFrame:
    print(f"Loading {params.local_file_name}...")
    combined_df_filepath = os.path.join(
        params.cache_dir,
        params.local_file_name
    )

    client      = bigquery.Client()
    file_prefix = str(random.getrandbits(128))
    filepath    = f"gs://{params.bucket_name}/{file_prefix}"
    export_table_to_gcs(client, params.table_id, filepath, 'US')

    create_cache_dir(params.cache_dir)

    print("Downloading data...")
    download_blobs_with_prefix(
        params.bucket_name, 
        file_prefix, 
        params.cache_dir
    )
    delete_blobs_in_gcs(params.bucket_name, str(params.table_id), params.cache_dir)
    print("Data downloaded completed.")

    csv_files = fetch_csvs(params.cache_dir, file_prefix)
    
    combined_df = create_df_from_csvs(params.cache_dir, csv_files)

    combined_df.to_csv(combined_df_filepath, index=False)

    delete_csvs(params.cache_dir, csv_files)

    print("Dataset merged.")
    return combined_df


def is_data_downloaded(local_file_name: str) -> bool:
    return os.path.exists(local_file_name)


def export_table_to_gcs(
    client:     bigquery.Client,
    table_name: str,
    filepath:   str,
    location:   str
    ) -> None:
    extract_job = client.extract_table(
        table_name, 
        f"{filepath}_*.csv",
        location=location
    )  
    extract_job.result()
    print(f"Exported table '{table_name}' to '{filepath}'.")


def create_cache_dir(cache_dir: str) -> None:
    os.makedirs(cache_dir, exist_ok=True)


def download_blobs_with_prefix(
    bucket_name:    str, 
    prefix:         str, 
    cache_dir:      str
    ) -> None:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        local_file_path = os.path.join(cache_dir, blob.name)
        blob.download_to_filename(local_file_path)
        print(f"Downloaded {blob.name} to {local_file_path}.")


def delete_blobs_in_gcs(
    bucket_name:    str, 
    prefix:         str, 
    cache_dir:      str
    ) -> None:
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    for blob in blobs:
        blob_name = blob.name
        blob.delete()
        print(f"Blob {blob_name} deleted.")


def fetch_csvs(cache_dir: str, file_prefix: str) -> List[str]:
    csv_files = [
        f 
        for f in os.listdir(cache_dir) 
        if f.endswith('.csv') and f.startswith(file_prefix)
    ]
    return csv_files


def create_df_from_csvs(cache_dir: str, csv_files: List[str]) -> pd.DataFrame:
    dfs = [pd.read_csv(os.path.join(cache_dir, file)) for file in csv_files]
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df


def delete_csvs(cache_dir: str, csv_files: List[str]) -> None:
    for file in csv_files:
        file_path = os.path.join(cache_dir, file)
        os.remove(file_path)
        print(f"Deleted file: {file_path}.")

