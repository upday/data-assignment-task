from unittest import TestCase


class TestExtractor(TestCase):

    def test_load_s3_oject_as_df(self):
        import warnings
        warnings.filterwarnings("ignore")
        from etl.Extractor import Extractor
        df = Extractor('upday-data-assignment', 'lake/2019-02-15.tsv').load_s3_oject_as_df()
        self.assert_(20000, len(df.index))
        self.assert_('article_viewed', df.head(1).EVENT_NAME)


