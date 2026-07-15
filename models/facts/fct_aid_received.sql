SELECT
    s.id AS aid_event_id,
    STRFTIME(s.received_date, '%Y%m%d')::INT AS date_id,
    s.number_of_trucks,
    dcargo.cargo_id,
    s.quantity,
    s.units,
    dc.crossing_id,
    s.data_period,
    s.last_edited
FROM {{ ref('stg_aid_received') }} s
LEFT JOIN {{ ref('dim_crossing') }} dc
ON s.crossing = LOWER(TRIM(dc.crossing_name))
LEFT JOIN {{ ref('dim_cargo') }} dcargo
ON s.items = dcargo.items
AND s.cargo_category = dcargo.cargo_category
AND s.donation_type = dcargo.donation_type
WHERE status = 'Received'