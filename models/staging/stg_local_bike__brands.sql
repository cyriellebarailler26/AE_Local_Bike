WITH source AS (
    SELECT * FROM {{ source('local_bike', 'brands') }}
),

renamed AS (
    SELECT
        CAST(brand_id AS STRING) AS brand_id,
        brand_name
    FROM source
)

SELECT * FROM renamed
