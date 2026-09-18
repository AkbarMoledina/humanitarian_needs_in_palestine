SELECT 
    aid_event_id
FROM {{ ref('fct_aid_received') }}
WHERE number_of_trucks <= 0
