SELECT
    {{ dbt_utils.generate_surrogate_key(['crossing_name']) }} AS crossing_id,
    crossing_name,
    border_zone,
    bordering_country,
    latitude,
    longitude
FROM {{ ref('crossing_seed') }}