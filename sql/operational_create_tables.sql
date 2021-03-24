BEGIN;

DROP TABLE IF EXISTS operational_db.article_performance CASCADE;

CREATE TABLE operational_db.article_performance (article_id VARCHAR(100) , "date" DATE ,
                                                title TEXT ,category VARCHAR(100) ,
                                                card_views INT ,article_views INT ,inserted_at TIMESTAMP);


DROP TABLE IF EXISTS operational_db.user_performance CASCADE;

CREATE TABLE operational_db.user_performance (user_id VARCHAR(100) , "date" DATE ,
                                              ctr FLOAT ,inserted_at TIMESTAMP);

COMMIT;