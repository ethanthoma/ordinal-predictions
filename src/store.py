import configparser
from dataclasses import dataclass, fields
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd


@dataclass
class TableInBigQueryParams:
    table_id: str

    dataset_id: str = None


    def __post_init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')

        for field in fields(self):
            if getattr(self, field.name) is None:
                section = field.name
                if section not in config:
                    section = __name__

                setattr(self, field.name, config.get(section, field.name))


def table_in_bigquery(params: TableInBigQueryParams):
    try:
        client = bigquery.Client()
        client.get_table(f'{params.dataset_id}.{params.table_id}')
        return True
    except NotFound:
        return False


@dataclass
class SaveToTableParams:
    df:         pd.DataFrame
    table_name: str

    dataset_id: str = None


    def __post_init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')

        for field in fields(self):
            if getattr(self, field.name) is None:
                section = field.name
                if section not in config:
                    section = __name__

                if field.type == int:
                    setattr(self, field.name, config.getint(section, field.name))
                else:
                    setattr(self, field.name, config.get(section, field.name))


def save_to_table(params: SaveToTableParams) -> None:
    print(f'Saving to bigquery in table {params.table_name}')
    client = bigquery.Client()
    table_id = f'{params.dataset_id}.{params.table_name}'

    try:
        job = client.load_table_from_dataframe(params.df, table_id)
        job.result()
    except Exception as e:
        print(f'Failed to save to table {table_id}: {e}')

