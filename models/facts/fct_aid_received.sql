SELECT
    id AS aid_event_id,
    STRFTIME(received_date, '%Y%m%d')::INT AS date_id,
    number_of_trucks,
    items,
    cargo_category,
    quantity,
    units,
    donation_type,
    crossing_id,
    data_period,
    last_edited
FROM {{ ref('stg_aid_received') }} s
LEFT JOIN {{ ref('dim_crossing') }} dc
ON s.crossing = LOWER(TRIM(dc.crossing_name))
WHERE status = 'Received'