SELECT
    {{ dbt_utils.generate_surrogate_key(['LOWER(TRIM(crossing_name))']) }} AS crossing_id,
    LOWER(TRIM(crossing_name)) AS crossing_name,
    border_zone,
    bordering_country,
    latitude,
    longitude
FROM {{ ref('crossing_seed') }}