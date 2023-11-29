import configparser
from dataclasses import dataclass, fields
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import seaborn as sns


@dataclass
class PlotCreatedParams:
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

                setattr(self, field.name, config.get(section, field.name))


def plot_created(params: PlotCreatedParams):
    plot_filepath = os.path.join(
        params.cache_dir,
        params.local_file_name + '.png'
    )

    return os.path.exists(plot_filepath)


@dataclass
class CreatePlotParams:
    df:                     pd.DataFrame
    local_file_name:        str

    cache_dir:              str = None
    target_column_names:    str = None
    date_column_name:       str = None


    def __post_init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')

        for field in fields(self):
            if getattr(self, field.name) is None:
                section = field.name
                if section not in config:
                    section = __name__

                setattr(self, field.name, config.get(section, field.name))


def create_plot(params: CreatePlotParams):
    plot_filepath = os.path.join(
        params.cache_dir,
        params.local_file_name + '.png'
    )

    target_column_name = next((col for col in params.target_column_names.split() if col in params.df.columns), None)
    if not target_column_name:
        raise ValueError("No valid target column name found in DataFrame.")

    # filter
    params.df[params.date_column_name] = pd.to_datetime(params.df[params.date_column_name])
    params.df['year'] = params.df[params.date_column_name].dt.year
    params.df = params.df[params.df['year'] >= 2009]

    # true data
    true_counts = params.df.groupby(['year', target_column_name]).size().unstack(fill_value=0)
    true_props = true_counts.divide(true_counts.sum(axis=1), axis=0)

    # predict data
    predicted_counts = pd.DataFrame()
    for star in range(1, 6):
        column_name = f'pred_{star}_pr'
        if column_name in params.df.columns:
            predicted_counts[star] = params.df.groupby('year')[column_name].sum()
    predicted_props = predicted_counts.divide(predicted_counts.sum(axis=1), axis=0)

    plt = plot(true_props, predicted_props, params.df['year'])
    plt.savefig(plot_filepath, bbox_inches='tight')


def plot(true_props, predicted_props, years):
    star_colors = {
        '5': '#2a83ba',
        '4': '#abdda4',
        '3': '#fffdbf',
        '2': '#fdae61',
        '1': '#d71e1d' 
    }

    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(20, 8), sharex=True)

    sorted_columns = sorted(true_props.columns, key=lambda x: str(x))
    cmap = [star_colors[str(col)] for col in sorted_columns]

    axes[0].stackplot(true_props.index, true_props.T, labels=true_props.columns, colors=cmap, alpha=0.8)
    axes[0].set_title('True Ratings')
    axes[0].set_ylabel('Proportion')
    axes[0].legend(loc='upper left')
    handles, labels = axes[0].get_legend_handles_labels()
    axes[0].legend(handles[::-1], labels[::-1], loc='center left', bbox_to_anchor=(1, 0.5), title='Stars', framealpha=0.5)

    axes[1].stackplot(predicted_props.index, predicted_props.T, labels=predicted_props.columns, colors=cmap, alpha=0.8)
    axes[1].set_title('Predicted Ratings')

    for ax in axes:
        ax.set_xlabel('Year')
        ax.grid(axis='y', color='lightgrey', linestyle='--', linewidth=0.7)
        ax.axvline(x=2013, color='grey', linestyle='--', linewidth=1)
        ax.set_xticks(range(2009, years.max() + 1, 2))
        ax.set_xticklabels(range(2009, years.max() + 1, 2), rotation=45)

    plt.tight_layout(rect=[0, 0, 0.85, 1])
    return plt
