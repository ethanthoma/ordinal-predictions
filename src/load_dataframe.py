from dataclasses import dataclass
import pandas as pd


@dataclass
class LoadChunksAsDfParams:
    cache_dir: str

def load_chunks_as_df(params: LoadChunksAsDfParams):
    csv_files = [f for f in os.listdir(params.cache_dir) if f.endswith('.csv')]
    dfs = [pd.read_csv(os.path.join(params.cache_dir, file)) for file in csv_files]
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df


@dataclass
class SplitDfParams:
    df:                 pd.DataFrame
    target_column_name: str

def split_df(params: SplitDfParams):
    return params.df.drop(params.target.column_name, axis=1), df[params.target.column_name]

