import configparser
from dataclasses import dataclass, fields
import mord
import os
import pandas as pd
from typing import List


@dataclass
class CreatePredictionsParams:
    df:                     pd.DataFrame
    local_file_name:        str

    cache_dir:              str = None
    target_column_names:    str = None
    drop_columns:           str = None
    random_state:           int = None
    date_column_name:       str = None
    date_filter_value:      int = None


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


def create_predictions(params: CreatePredictionsParams) -> pd.Series:
    combined_df_filepath = os.path.join(
        params.cache_dir,
        params.local_file_name
    )

    if os.path.exists(combined_df_filepath):
        print(f"{params.local_file_name} already exists, returning...")
        return pd.read_csv(combined_df_filepath)


    target_column_names = params.target_column_names.split(' ')
    drop_columns = params.drop_columns.split(' ')

    target_column_name = next(
        (col for col in target_column_names if col in params.df.columns),
        None
    )

    if target_column_name is None:
        print('No target column found in dataframe')
        return

    params.df[params.date_column_name] = pd.to_datetime(params.df[params.date_column_name])
    training_df = params.df[params.df[params.date_column_name].dt.year == params.date_filter_value]

    train_X = get_X(training_df, target_column_name, drop_columns)
    train_y = get_y(training_df, target_column_name)

    model = mord.LogisticAT(alpha=1.)
    model.fit(train_X, train_y)

    X = get_X(params.df, target_column_name, drop_columns)

    params.df['predicted'] = model.predict(X)

    params.df.to_csv(combined_df_filepath, index=False)
    return params.df


def get_X(df: pd.DataFrame, target_column_name: str, drop_columns: List[str]) -> pd.DataFrame:
    return df.drop([target_column_name] + drop_columns, axis=1)


def get_y(df: pd.DataFrame, target_column_name: str) -> pd.Series:
    return df[target_column_name]
