SELECT
    id,
    number_of_trucks,
    STRFTIME(received_date, '%Y%m%d')::INT AS date_id,
    items,
    cargo_category,
    status,
    quantity,
    units,
    donated_by,
    donation_type,
    crossing,
    destination_recipient,
    data_period,
    last_edited
FROM {{ ref('stg_aid_received')}}
WHERE status = 'Received'