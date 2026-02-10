WITH org_names AS (
    SELECT TRIM(UNNEST(STRING_SPLIT(donated_by, ' + '))) AS organisation_name
    FROM {{ ref('stg_aid_received') }}
    WHERE donated_by IS NOT NULL AND donated_by != ''

    UNION

    SELECT TRIM(UNNEST(STRING_SPLIT(recipient, ' + '))) AS organisation_name
    FROM {{ ref('stg_aid_received') }}
    WHERE recipient IS NOT NULL AND recipient != ''
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['organisation_name']) }} AS org_id,
    organisation_name
FROM org_names
WHERE organisation_name != ''
ORDER BY organisation_name