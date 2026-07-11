SELECT
    d.date_id,
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.quarter,
    d.week_of_year,
    COUNT(f.aid_event_id) AS number_of_aid_events,
    COALESCE(SUM(f.number_of_trucks), 0)::INT AS total_trucks
FROM {{ ref('dim_date') }} d
LEFT JOIN {{ ref('fct_aid_received') }} f
ON d.date_id = f.date_id
GROUP BY d.date_id, d.full_date, d.year, d.month, d.month_name, d.quarter, d.week_of_year
ORDER BY d.full_date