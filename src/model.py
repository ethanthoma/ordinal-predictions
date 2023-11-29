import configparser
from dataclasses import dataclass, fields
import os
import pandas as pd
import statsmodels.api as sm
from statsmodels.miscmodels.ordinal_model import OrderedModel
from typing import List, Optional


@dataclass
class PredictionsCreatedParams:
    local_file_name:        str

    cache_dir:              str = None


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


def predictions_created(params: PredictionsCreatedParams):
    combined_df_filepath = os.path.join(
        params.cache_dir,
        params.local_file_name
    )

    return os.path.exists(combined_df_filepath)


@dataclass
class FetchPredictionsParams:
    local_file_name:        str

    cache_dir:              str = None


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


def fetch_predictions(params: FetchPredictionsParams):
    combined_df_filepath = os.path.join(
        params.cache_dir,
        params.local_file_name
    )

    if os.path.exists(combined_df_filepath):
        print(f"{params.local_file_name} already exists, returning...")
        return pd.read_csv(combined_df_filepath)


@dataclass
class CreatePredictionsParams:
    df:                     pd.DataFrame
    local_file_name:        str

    cache_dir:              str = None
    target_column_names:    str = None
    drop_columns:           str = None
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


    drop_columns = params.drop_columns.split(' ')

    target_column_name = get_target_column_name(
        params.df.columns,
        params.target_column_names
    )

    if target_column_name is None:
        print('No target column found in dataframe')
        return

    params.df[params.date_column_name] = pd.to_datetime(params.df[params.date_column_name])
    training_df = params.df[params.df[params.date_column_name].dt.year == params.date_filter_value]

    train_X = get_X(training_df, target_column_name, drop_columns)
    train_y = get_y(training_df, target_column_name)

    model = OrderedModel(train_y, train_X, distr='logit')
    fit = model.fit(method='bfgs')

    X = get_X(params.df, target_column_name, drop_columns)
    params.df = params.df.drop(X.columns, axis=1, errors='ignore')

    pred_probs = fit.predict(X)
    pred_probs.columns = [f'pred_{int(col) + 1}_pr' for col in pred_probs.columns]
    params.df = pd.concat([params.df, pred_probs], axis=1)

    params.df['predicted'] = pred_probs.idxmax(axis=1)

    print(params.df)

    save_model_summary(fit, params.cache_dir, f'{params.local_file_name}_summary.txt')

    params.df.to_csv(combined_df_filepath, index=False)
    return params.df


def get_target_column_name(
    column_names:           List[str],
    target_column_names:    str
    ) -> Optional[str]:
    all_possible_names = target_column_names.split(' ')
    return next(
        (
            col 
            for col in all_possible_names 
            if col in column_names
        ),
        None
    )


def get_X(df: pd.DataFrame, target_column_name: str, drop_columns: List[str]) -> pd.DataFrame:
    columns_to_drop = [target_column_name] + drop_columns
    return df.drop(columns_to_drop, axis=1, errors='ignore')


def get_y(df: pd.DataFrame, target_column_name: str) -> pd.Series:
    return df[target_column_name]


def save_model_summary(fit, cache_dir: str, local_file_name: str) -> None:
    local_filepath = os.path.join(
        cache_dir,
        local_file_name
    )

    with open(local_filepath, 'w') as f:
        f.write(fit.summary().as_text())

