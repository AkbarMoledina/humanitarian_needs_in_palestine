SELECT
    aid_event_id,
    f.date_id,
    d.full_date,
    d.year,
    d.week_of_year,
    number_of_trucks,
    items,
    cargo_category,
    quantity,
    units,
    donation_type,
    crossing,
    data_period,
    last_edited
FROM fct_aid_received f
INNER JOIN {{ ref('dim_date') }} d
ON f.date_id = d.date_id
