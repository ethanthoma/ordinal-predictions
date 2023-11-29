import configparser

from fetch import (
    DataFetchedParams, data_fetched,
    LoadFetchedDataParams, load_fetched_data,
    FetchFromTableParams, fetch_from_table
)
from model import (
    PredictionsCreatedParams, predictions_created, 
    FetchPredictionsParams, fetch_predictions,
    CreatePredictionsParams, create_predictions
)
from store import TableInBigQueryParams, table_in_bigquery, SaveToTableParams, save_to_table
from plot import PlotCreatedParams, plot_created, CreatePlotParams, create_plot


def data(fetch_table_id, local_file_name):
    if data_fetched(DataFetchedParams(local_file_name)):
        return load_fetched_data(LoadFetchedDataParams(local_file_name))
    else: 
        return fetch_from_table(FetchFromTableParams(fetch_table_id, local_file_name))


def predictions(fetch_table_id, local_file_name):
    predictions_file_name = f'{local_file_name}_predictions'
    if predictions_created(PredictionsCreatedParams(predictions_file_name)):
        return fetch_predictions(FetchPredictionsParams(predictions_file_name))
    else:
        df = data(fetch_table_id, local_file_name)
        return create_predictions(CreatePredictionsParams(df, predictions_file_name))


def table(local_file_name, fetch_table_id):
    store_table_id = f'{local_file_name}_predictions'

    if table_in_bigquery(TableInBigQueryParams(store_table_id)):
        print(f'The table {store_table_id} already exists.')
        return

    df = predictions(fetch_table_id, local_file_name)
    save_to_table(SaveToTableParams(df, store_table_id))


def plot(local_file_name, fetch_table_id):
    plot_name = f'{local_file_name}_plot'
    if plot_created(PlotCreatedParams(plot_name)):
        print(f'The plot {plot_name} already exists.')
        return
    
    df = predictions(fetch_table_id, local_file_name)
    create_plot(CreatePlotParams(df, plot_name))


if __name__ == '__main__':
    config = configparser.ConfigParser(default_section=None)
    config.read('config.ini')

    for local_file_name, fetch_table_id in config['table_ids'].items():
        table(local_file_name, fetch_table_id)
        plot(local_file_name, fetch_table_id)

