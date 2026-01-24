WITH source AS (
    SELECT * FROM {{ source('raw', 'aid_received')}}
)

SELECT
    ID AS id,
    "No. of Trucks" AS number_of_trucks,
    "Received Date"::DATE AS received_date,
    "Description of Cargo" AS items,
    "Cargo Category" AS cargo_category,
    Status AS status,
    Quantity AS quantity,
    Units AS units,
    "Donating Country/ Organization" AS donated_by,
    "Donation Type" AS donation_type,
    Crossing AS crossing,
    "Destination Recipient/ Partner" AS destination_recipient,
    "Data Period" AS data_period,
    "Last Edited Time"::TIMESTAMP AS last_edited
FROM source
