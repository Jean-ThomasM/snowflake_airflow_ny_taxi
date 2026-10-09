SELECT COUNT(*) = 0
FROM (
    SELECT vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, pulocationid, dolocationid, trip_distance, total_amount, COUNT(*) 
    FROM NYC_TAXI.RAW.YELLOW_TRIPDATA 
    WHERE _source_file = 'yellow_tripdata_{{ logical_date.strftime("%Y-%m") }}.parquet'
    GROUP BY ALL
    HAVING COUNT(*) > 1
);
