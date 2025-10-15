WITH source AS (
    SELECT * FROM {{ source('local_bike', 'stocks') }}
),

renamed AS (
    SELECT
        CAST(store_id AS STRING) AS store_id,
        CAST(product_id AS STRING) AS product_id,
        quantity
    FROM source
)

SELECT * FROM renamed
