WITH source AS (
    SELECT * FROM {{ source('local_bike', 'categories') }}
),

renamed AS (
    SELECT
        CAST(category_id AS STRING) AS category_id,
        category_name
    FROM source
)

SELECT * FROM renamed
