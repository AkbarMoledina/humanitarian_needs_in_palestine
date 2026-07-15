WITH aid_date_range AS (
    SELECT
        MIN(dd.full_date) AS min_date,
        MAX(dd.full_date) AS max_date
    FROM {{ ref('fct_aid_received') }} f
    JOIN {{ ref('dim_date') }} dd
      ON f.date_id = dd.date_id
),

month_spine AS (
    SELECT DISTINCT
        year,
        month,
        month_name
    FROM {{ ref('dim_date') }}
    WHERE full_date BETWEEN (SELECT min_date FROM aid_date_range)
                       AND (SELECT max_date FROM aid_date_range)
),

cargo_categories AS (
    SELECT DISTINCT dcargo.cargo_category
    FROM {{ ref('fct_aid_received') }} f
    JOIN {{ ref('dim_cargo') }} dcargo ON f.cargo_id = dcargo.cargo_id
    WHERE dcargo.cargo_category IS NOT NULL
),

cross_join AS (
    SELECT
        ms.year,
        ms.month,
        ms.month_name,
        cc.cargo_category
    FROM month_spine ms
    CROSS JOIN cargo_categories cc
),

monthly_trucks AS (
    SELECT
        dd.year,
        dd.month,
        dd.month_name,
        dcargo.cargo_category,
        SUM(f.number_of_trucks) AS monthly_truck_count
    FROM {{ ref('fct_aid_received') }} f
    JOIN {{ ref('dim_date') }} dd ON f.date_id = dd.date_id
    JOIN {{ ref('dim_cargo') }} dcargo ON f.cargo_id = dcargo.cargo_id
    WHERE f.number_of_trucks IS NOT NULL
    GROUP BY dd.year, dd.month, dd.month_name, dcargo.cargo_category
)

SELECT
    cj.year,
    cj.month,
    cj.month_name,
    cj.cargo_category,
    DATE_TRUNC('month', DATE(cj.year || '-' || cj.month || '-01')) AS month_start,
    COALESCE(mt.monthly_truck_count, 0) AS monthly_truck_count
FROM cross_join cj
LEFT JOIN monthly_trucks mt
  ON cj.year = mt.year
  AND cj.month = mt.month
  AND cj.cargo_category = mt.cargo_category
ORDER BY cj.year, cj.month, cj.cargo_category
