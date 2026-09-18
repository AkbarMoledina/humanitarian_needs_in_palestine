SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['items', 'cargo_category']) }} AS cargo_id,
    items,
    cargo_category,
FROM {{ ref('stg_aid_received') }}
WHERE items IS NOT NULL
    and items != ''