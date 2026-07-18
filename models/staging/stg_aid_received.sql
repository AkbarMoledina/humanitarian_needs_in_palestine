WITH source AS (
    SELECT * FROM {{ source('raw', 'aid_received')}}
)

SELECT
    ID AS id,
    "No. of Trucks" AS number_of_trucks,
    "Received Date"::DATE AS received_date,
    "Description of Cargo" AS items,
    COALESCE("Cargo Category", 'Unknown') AS cargo_category,
    Status AS status,
    Quantity AS quantity,
    Units AS units,
    "Donating Country/ Organization" AS donated_by,
    COALESCE("Donation Type", 'Unknown') AS donation_type,
    LOWER(TRIM(Crossing)) AS crossing,
    "Destination Recipient/ Partner" AS recipient,
    "Data Period" AS data_period,
    "Last Edited Time"::TIMESTAMP AS last_edited,
    CURRENT_TIMESTAMP AS loaded_at
FROM source
