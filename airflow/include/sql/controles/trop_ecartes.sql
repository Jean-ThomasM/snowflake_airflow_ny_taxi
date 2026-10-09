SELECT (
    SUM(CASE WHEN rejection_reason IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*)
) < 0.20
FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__FLAGGED
WHERE source_file_month = '{{ ds }}'::date;
