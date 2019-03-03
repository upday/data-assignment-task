import os
import pandas as pd
from unittest import TestCase

from etl.transformer import Transformer


class TestTransform(TestCase):

    def setUp(self):
        columns = ['TIMESTAMP', 'MD5(SESSION_ID)', 'EVENT_NAME', 'MD5(USER_ID)', 'ATTRIBUTES']
        data = [
            ['2019-02-15 03:07:03.662 +0000', '1', 'my_news_card_viewed', '1',
             '{ "category": "digital_life", "id": "jLcW_MnCfbLN8Ph-bsTwGw", "noteType": "TRENDING_SOCIAL", "orientation": "PORTRAIT", "position": "1", "publishTime": "2019-02-14T21:08:00Z", "sourceDomain": "gamestar.de", "sourceName": "GameStar", "stream": "wtk", "streamType": "my news", "subcategories": [ "digital_life.games" ], "title": "News: Overwatch League - 2. Saison startet heute Nacht, ein einziger deutscher Spieler ist dabei", "url": "https://www.gamestar.de/artikel/overwatch-league-2-saison-startet-heute-nacht-ein-einziger-deutscher-spieler-ist-dabei,3340648.amp" }'],
            ['2019-02-15 03:07:03.662 +0000', '2', 'my_news_card_viewed', '2',
             '{ "category": "fashion_beauty_lifestyle", "id": "3HeUw-OQYiWnQpFjqTXobQ", "noteType": "SPECIFIC_TRENDING", "orientation": "PORTRAIT", "position": "9", "publishTime": "2019-02-14T12:22:00Z", "sourceDomain": "merkur.de", "sourceName": "Merkur.de", "stream": "wtk", "streamType": "my news", "subcategories": [ "fashion_beauty_lifestyle.health" ], "title": "So bekommen Sie die Nase in einer Minute frei - ohne Nasenspray", "url": "https://www.merkur.de/leben/gesundheit/bekommen-nase-einer-minute-frei-ohne-nasenspray-zr-11764797.amp.html" }'],
            ['2019-02-15 03:07:03.662 +0000', '3', 'article_viewed', '3',
             '{ "browser": "Chrome", "category": "digital_life", "id": "-gpe6V_-in4hYf8oM-twaQ", "network": "online", "noteType": "TRENDING_NEWS", "publishTime": "2019-02-08T20:25:00Z", "sourceDomain": "winfuture.mobi", "sourceName": "WinFuture", "stream": "wtk", "subcategories": [ "digital_life.mobile_tablet" ], "title": "Die Smartphones mit der höchsten Strahlung", "url": "https://winfuture.de/infografik/19938/Die-Smartphones-mit-der-hoechsten-Strahlung-1549285618.html" }'],
            ['2019-02-15 03:07:03.662 +0000', '3', 'random', '4',
             '{ "category": "fashion_beauty_lifestyle", "id": "X1WA8VD8lhFKh0LhtskgtA", "noteType": "SPECIFIC_NEWS", "orientation": "PORTRAIT", "position": "275", "sourceDomain": "frag-mutti.de", "sourceName": "Frag-Mutti.de", "stream": "wtk", "streamType": "my news", "title": "Aus leeren Plastikbehältern werden bunte Dosen für Bastelzubehör", "url": "https://www.frag-mutti.de/aus-leeren-plastikbehaeltern-werden-bunte-dosen-fuer-bastelzubehoer-a54419/" }'],

        ]
        self.df = pd.DataFrame(data, columns=columns)

    def test___filter_unrelevant_events(self):
        transformer = Transformer(self.df)
        result = transformer.transform()
        self.assert_(3, len(result.index))

    def test__flatten_attributes_to_columns(self):
        transformer = Transformer(self.df)
        result = transformer.transform()
        self.assert_('digital_life', result.category.head(1)[0])

    def test__save_dim_user(self):
        transformer = Transformer(self.df)
        result = transformer.transform()
        Transformer.PersistHelper(df=result, file_loc='test/results').save_df_to_filesystem()
        self.assertTrue(os.path.isfile('test/results/fact_article_access.csv'))
        self.assertTrue(os.path.isfile('test/results/fact_user_activity.csv'))

