BEGIN
    BEGIN TRANSACTION;
    MERGE INTO `{project_id}.{bigqury_dataset}.{bigqury_fact_table}` AS F 
        USING `{project_id}.{bigqury_dataset}.temp_{bigqury_fact_table}` AS T 
        ON F.dead_id = T.dead_id
        AND F.dead_year = T.dead_year
        WHEN MATCHED THEN
        UPDATE SET
            F.dead_year = T.dead_year,
            F.age = T.age,
            F.sex = T.sex,
            F.risk_helmet = T.risk_helmet,
            F.risk_safetybelt = T.risk_safetybelt,
            F.dead_date_final = T.dead_date_final,
            F.date_rec = T.date_rec,
            F.time_rec = T.time_rec,
            F.cause = T.cause,
            F.vehicle_merge_final = T.vehicle_merge_final,
            F.location.sub_district = T.location.sub_district,
            F.location.district = T.location.district,
            F.location.province = T.location.province,
            F.location.latitude = T.location.latitude,
            F.location.longitude = T.location.longitude
        WHEN NOT MATCHED THEN
        INSERT
        (
            dead_id,
            dead_year,
            age,
            sex,
            risk_helmet,
            risk_safetybelt,
            dead_date_final,
            date_rec,
            time_rec,
            cause,
            vehicle_merge_final,
            location
        )
        VALUES
        (
            T.dead_id,
            T.dead_year,
            T.age,
            T.sex,
            T.risk_helmet,
            T.risk_safetybelt,
            T.dead_date_final,
            T.date_rec,
            T.time_rec,
            T.cause,
            T.vehicle_merge_final,
            T.location
        );
    COMMIT TRANSACTION;
  END;
  
DROP TABLE `{project_id}.{bigqury_dataset}.temp_{bigqury_fact_table}`;