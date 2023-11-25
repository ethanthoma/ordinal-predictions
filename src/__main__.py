import configparser

from bigquery_fetch import LoadTableAsChunksParams, load_table_as_chunks
from load_dataframe import (
    LoadChunksAsDf, load_chunks_as_df, 
    SplitDfParams, split_df
)


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')

    cache_dir   = config.get('DEFAULT', 'cache_dir')

    dataset_id  = config.get('bigquery', 'dataset_id')
    table_id    = config.get('bigquery', 'table_id')
    chunk_size  = config.getint('bigquery', 'chunk_size')
    load_table_as_chunks(LoadTableAsChunksParams(
        dataset_id,
        table_id,
        cache_dir,
        chunk_size
    ))

    df = load_chunks_as_df(LoadChunksAsDfParams(
        cache_dir
    ))

    target_column_name = config.get('data', 'target_column_name')
    X, y = split_df(SplitDfParams(
        df,
        target_column_name
    ))

    random_state = config.getInt('models', 'random_state')
    X_train, X_test, y_train, y_test = train_test_split(
        params.X, 
        params.y, 
        test_size=0.2, 
        random_state=params.random_state
    )

    model = ordered_logistic_regression(OrderedLogisticRegressionParams(
        X_train,
        y_train
    ))

    predictions = model.predict(X_test)
    print("Accuracy:", accuracy_score(y_test, predictions))

