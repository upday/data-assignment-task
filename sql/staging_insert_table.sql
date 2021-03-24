BEGIN;

CREATE TABLE staging_db.performance_for_insert (LIKE staging_db.stg_performance);

DROP TABLE IF EXISTS partitions_in_source;

CREATE
TEMPORARY TABLE partitions_in_source AS
SELECT DISTINCT bucketname,
                itemname
FROM staging_db.performance_for_insert;


INSERT INTO staging_db.performance_for_insert
SELECT timestamp_::TIMESTAMP ,
       md5_session_id ,
       event_name ,
       md5_user_id ,
       attributes ,
       bucketname ,
       itemname ,
       inserted_at ,
       attributes_category ,
       attributes_id ,
       attributes_notetype ,
       attributes_orientation ,
       attributes_position ,
       attributes_publishtime ,
       attributes_sourcedomain ,
       attributes_sourcename ,
       attributes_stream ,
       attributes_streamtype ,
       attributes_subcategories ,
       attributes_title ,
       attributes_url ,
       attributes_browser ,
       attributes_network ,
       attributes_from ,
       attributes_feature ,
       attributes_note_type ,
       attributes_source ,
       attributes_source_id ,
       attributes_stream_type ,
       attributes_type ,
       attributes_format ,
       attributes_provider ,
       attributes_navigatedto ,
       attributes_navigatedfrom ,
       attributes_interesting ,
       attributes_breakingnewsenabled ,
       attributes_adtype ,
       attributes_rank ,
       attributes_action ,
       attributes_personalisednewsenabled ,
       attributes_searchterm ,
       attributes_status ,
       attributes_updayresults ,
       attributes_frompush
FROM
  (SELECT timestamp_ ,
          md5_session_id ,
          event_name ,
          md5_user_id ,
          attributes ,
          bucketname ,
          itemname ,
          inserted_at ,
          attributes_category ,
          attributes_id ,
          attributes_notetype ,
          attributes_orientation ,
          attributes_position ,
          attributes_publishtime ,
          attributes_sourcedomain ,
          attributes_sourcename ,
          attributes_stream ,
          attributes_streamtype ,
          attributes_subcategories ,
          attributes_title ,
          attributes_url ,
          attributes_browser ,
          attributes_network ,
          attributes_from ,
          attributes_feature ,
          attributes_note_type ,
          attributes_source ,
          attributes_source_id ,
          attributes_stream_type ,
          attributes_type ,
          attributes_format ,
          attributes_provider ,
          attributes_navigatedto ,
          attributes_navigatedfrom ,
          attributes_interesting ,
          attributes_breakingnewsenabled ,
          attributes_adtype ,
          attributes_rank ,
          attributes_action ,
          attributes_personalisednewsenabled ,
          attributes_searchterm ,
          attributes_status ,
          attributes_updayresults ,
          attributes_frompush ,
          row_number() OVER (PARTITION BY timestamp_,
                                          md5_session_id,
                                          event_name,
                                          md5_user_id
                             ORDER BY timestamp_ DESC) nr
   FROM staging_db.stg_performance stg
   LEFT JOIN partitions_in_source src USING (bucketname,
                                             itemname)
   WHERE src.bucketname IS NULL
     AND src.itemname IS NULL ) AS dedup_step
WHERE nr=1;

COMMIT;