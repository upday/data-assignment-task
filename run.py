def extract():
    from etl.extractor import Extractor
    return Extractor('upday-data-assignment', 'lake/').load_s3_oject_as_df()


def transform(df):
    from etl.transformer import Transformer
    result = Transformer(df).transform()
    Transformer.PersistHelper(result, 'results').save_df_to_filesystem()


def load():
    from etl.loader import Loader
    Loader().load_data_to_postgres()


if __name__ == '__main__':
    import time
    from etl.initializer import create_load_tables

    time.sleep(15)

    create_load_tables()
    df = extract()
    transform(df)
    load()
