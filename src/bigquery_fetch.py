from dataclasses import dataclass

from google.cloud import bigquery
import pandas as pd
import os


import warnings
warnings.filterwarnings(
    "ignore", 
    "Your application has authenticated using end user credentials"
)


@dataclass
class LoadTableAsChunksParams:
    dataset_id: str
    table_id:   str
    cache_dir:  str
    chunk_size: int


def load_table_as_chunks(params: LoadTableAsChunksParams) -> None:
    print("loading data...")
    if is_cache_used(params.cache_dir):
        print("cache folder not empty, no fetching required")
        return

    create_cache_dir(params.cache_dir)

    client = bigquery.Client()

    pages = client.list_rows(params.table_id, page_size=params.chunk_size).pages
    for index, page in enumerate(pages):
        print(f'fetching {index}...')
        df = pd.DataFrame([dict(row) for row in page])

        filename = f"chunk_{index}.csv"
        save_chunk_to_file(params.cache_dir, df, filename)

    print("Data loading completed.")


def is_cache_used(cache_dir: str):
    return os.path.exists(cache_dir) and os.path.isdir(cache_dir) and len(os.listdir(cache_dir)) > 0


def create_cache_dir(cache_dir: str) -> None:
    os.makedirs(cache_dir, exist_ok=True)


def save_chunk_to_file(
    cache_dir: str, 
    chunk: pd.DataFrame,
    filename: str
) -> None:
    filepath = os.path.join(cache_dir, filename)
    chunk.to_csv(filepath, index=False)

