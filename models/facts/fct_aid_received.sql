SELECT
    s.id AS aid_event_id,
    STRFTIME(s.received_date, '%Y%m%d')::INT AS date_id,
    s.number_of_trucks,
    s.items,
    s.cargo_category,
    s.quantity,
    s.units,
    s.donation_type,
    dc.crossing_id,
    s.data_period,
    s.last_edited
FROM {{ ref('stg_aid_received') }} s
LEFT JOIN {{ ref('dim_crossing') }} dc
ON s.crossing = LOWER(TRIM(dc.crossing_name))
WHERE status = 'Received'