import configparser

from bigquery_fetch import LoadTableAsChunksParams, load_table_as_chunks


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')

    value = config.get('bigquery', 'project_name')
    print(value)

    dataset_id  = config.get('bigquery', 'dataset_id')
    table_id    = config.get('bigquery', 'table_id')
    cache_dir   = config.get('bigquery', 'cache_dir')
    chunk_size  = config.getint('bigquery', 'chunk_size')

    print(chunk_size, cache_dir)
    params = LoadTableAsChunksParams(dataset_id, table_id, cache_dir, chunk_size)
    load_table_as_chunks(params)

