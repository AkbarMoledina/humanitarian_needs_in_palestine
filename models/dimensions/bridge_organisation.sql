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
    CROSS JOIN UNNEST(STRING_SPLIT(recipient, ' + ')) AS t(org)
    WHERE TRIM(t.org) != ''
      AND status = 'Received'
)

SELECT 
    a.aid_event_id,
    d.org_id,
    a.organisation_role
FROM aid_organisations a
INNER JOIN {{ ref('dim_organisation') }} d
    ON a.organisation_name = d.organisation_name