SELECT
    id AS aid_event_id,
    STRFTIME(received_date, '%Y%m%d')::INT AS date_id,
    number_of_trucks,
    items,
    cargo_category,
    quantity,
    units,
    donated_by,
    donation_type,
    crossing,
    recipient,
    data_period,
    last_edited
FROM {{ ref('stg_aid_received')}} s
WHERE status = 'Received'