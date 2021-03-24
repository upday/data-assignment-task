import pandas as pd
from dwh.extraction import explode_column


def test_explode_column():

    data='{"name":"gopi", "city":"berlin"}'
    df_test=pd.DataFrame([{'data': data}])
    df_test=explode_column(df_test,"data")

    df_expected=pd.DataFrame([{'data': data,'name':'gopi','city':'berlin'}])

    assert df_test.values.all()==df_expected.values.all()