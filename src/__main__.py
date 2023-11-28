import configparser

from fetch_from_table import FetchFromTableParams, fetch_from_table
from create_predictions import CreatePredictionsParams, create_predictions
from save_to_table import SaveToTableParams, save_to_table
from create_plot import CreatePlotParams, create_plot


if __name__ == '__main__':
    config = configparser.ConfigParser(default_section=None)
    config.read('config.ini')

    for table_name, table_id in config['table_ids'].items():
        df = fetch_from_table(FetchFromTableParams(table_id, table_name))

        print(f'{table_name} has size of {df.shape}')

        table_id = f'{table_name}_predictions'

        df = create_predictions(CreatePredictionsParams(df, table_id))

        save_to_table(SaveToTableParams(df, table_id))

        plot_name = f'{table_name}_plot'
        create_plot(CreatePlotParams(df, plot_name))

