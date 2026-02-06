WITH base AS (
    SELECT DISTINCT
        commodity_name,
        unit_amount
    FROM {{ ref('stg_commodity_prices') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'commodity_name',
        'unit_amount'
    ]) }} AS commodity_id,
    commodity_name,
    unit_amount,
    CASE
        WHEN commodity_name LIKE '%rice%' THEN 'Carbohydrates'
        WHEN commodity_name IN ('bread', 'biscuits', 'crushed bulgur', 'crushed dry freekeh', 'white canned cooked beans', 'egyptian beans medames') THEN 'Carbohydrates'
        WHEN commodity_name IN ('eggs', 'chickens', 'fresh lamb with bone', 'fresh veal') THEN 'Proteins'
        WHEN commodity_name IN ('apples', 'tomatoes', 'zucchinis', 'eggplants', 'chili pepper', 'bell pepper', 'cucumbers', 'dry onions', 'potato', 'lemons') THEN 'Fruit/vegetables'
        WHEN commodity_name LIKE '%flour%' THEN 'Staple ingredients'
        WHEN commodity_name IN ('oil', 'pure white sugar', 'crushed red lentils') THEN 'Staple ingredients'
        WHEN commodity_name IN ('cheese', 'white table salt', 'white yeast', 'tomato paste', 'ground coffee', 'baby milk powder') THEN 'Other food items'
        WHEN commodity_name IN ('gasoline', 'gas cylinder', 'diesel') THEN 'Fuel'
        WHEN commodity_name LIKE '%cigarette%' THEN 'Cigarettes'
        WHEN commodity_name LIKE '%water%' THEN 'Water'
        WHEN commodity_name LIKE '%travel%' THEN 'Travel'
        ELSE 'Other'
    END AS commodity_category
FROM base
