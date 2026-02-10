WITH aid_organisations AS (
    -- Donors
    SELECT 
        id AS aid_event_id,
        TRIM(t.org) AS organisation_name,
        'donor' AS organisation_role
    FROM {{ ref('stg_aid_received')}}
    CROSS JOIN UNNEST(STRING_SPLIT(donated_by, ' + ')) AS t(org)
    WHERE TRIM(t.org) != ''
      AND status = 'Received'
    
    UNION ALL
    
    -- Recipients  
    SELECT 
        id AS aid_event_id,
        TRIM(t.org) AS organisation_name,
        'recipient' AS organisation_role
    FROM {{ ref('stg_aid_received')}}
    CROSS JOIN UNNEST(STRING_SPLIT(destination_recipient, ' + ')) AS t(org)
    WHERE TRIM(t.org) != ''
      AND status = 'Received'
)

SELECT 
    ao.aid_event_id,
    do.org_id,
    ao.organisation_role
FROM aid_organisations ao
INNER JOIN {{ ref('dim_organisation') }} do 
    ON ao.organisation_name = do.organisation_name