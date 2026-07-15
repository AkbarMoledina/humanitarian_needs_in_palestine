SELECT
    f.aid_event_id,
    f.date_id,
    dd.full_date,
    dd.year,
    dd.week_of_year,
    dd.is_ceasefire,
    f.number_of_trucks,
    dcargo.items,
    dcargo.cargo_category,
    f.quantity,
    f.units,
    dcargo.donation_type,
    f.crossing_id,
    dc.crossing_name,
    f.data_period,
    f.last_edited
FROM {{ ref('fct_aid_received') }} f
LEFT JOIN {{ ref('dim_date') }} dd
    ON f.date_id = dd.date_id
LEFT JOIN {{ ref('dim_crossing') }} dc
    ON f.crossing_id = dc.crossing_id
LEFT JOIN {{ ref('dim_cargo') }} dcargo
    ON f.cargo_id = dcargo.cargo_id
ORDER BY dd.full_date
