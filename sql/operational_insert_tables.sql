BEGIN;

TRUNCATE TABLE operational_db.article_performance;

INSERT INTO operational_db.article_performance(article_id , "date" , title , category ,
                            card_views , article_views , inserted_at)

SELECT attributes_id AS article_id ,
       timestamp_::DATE AS "date" ,
       attributes_title AS title ,
       attributes_category AS category ,
       SUM(CASE
               WHEN event_name IN ('my_news_card_viewed', 'top_news_card_viewed') THEN 1
               ELSE 0
           END) AS card_views ,
       SUM(CASE
               WHEN event_name = 'article_viewed' THEN 1
               ELSE 0
           END) AS article_views ,
       CURRENT_TIMESTAMP AS inserted_at
FROM staging_db.performance_for_insert
WHERE event_name in ('my_news_card_viewed',
                     'top_news_card_viewed',
                     'article_viewed')
GROUP BY article_id ,
         "date" ,
         title ,
         category ,
         inserted_at ;


TRUNCATE TABLE operational_db.user_performance;

INSERT INTO operational_db.user_performance(user_id , "date" , ctr , inserted_at) WITH card_viewed AS
  (SELECT md5_user_id AS user_id,
          md5_session_id AS session_id,
          timestamp_::DATE AS "date",
          attributes_id
   FROM staging_db.performance_for_insert
   WHERE event_name in ('my_news_card_viewed',
                        'top_news_card_viewed')
     AND md5_session_id IS NOT NULL),
  article_viewed AS
  (SELECT md5_user_id AS user_id ,
          md5_session_id AS session_id ,
          timestamp_::DATE AS "date" ,
          attributes_id
   FROM staging_db.performance_for_insert
   WHERE event_name='article_viewed'
     AND md5_session_id IS NOT NULL),
  merged AS
  (SELECT c.user_id ,
          c."date" ,
          c.attributes_id ,
          coalesce(CASE
                       WHEN a.attributes_id IS NOT NULL THEN 1
                       ELSE 0
                   END) AS viewed
   FROM card_viewed c
   LEFT JOIN article_viewed a USING (user_id,
                                     "date",
                                     session_id,
                                     attributes_id))
SELECT user_id,
       "date",
       Sum(viewed)*1.0/count(attributes_id)*1.0 AS ctr,
       CURRENT_TIMESTAMP AS inserted_at
FROM merged
GROUP BY user_id,
         "date",
         inserted_at;

COMMIT;