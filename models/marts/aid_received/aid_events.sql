SELECT
    aid_event_id,
    f.date_id,
    dd.full_date,
    dd.year,
    dd.week_of_year,
    number_of_trucks,
    items,
    cargo_category,
    quantity,
    units,
    donation_type,
    f.crossing_id,
    dc.crossing_name,
    data_period,
    last_edited
FROM fct_aid_received f
RIGHT JOIN {{ ref('dim_date') }} dd
ON f.date_id = dd.date_id
INNER JOIN {{ ref('dim_crossing' )}} dc
ON f.crossing_id = dc.crossing_id
