WITH source AS (
    SELECT * FROM {{ source('local_bike', 'order_items') }}
),

renamed AS (
    SELECT
        CAST(order_id AS STRING) AS order_id,
        CAST(item_id AS STRING) AS item_id,
        CAST(product_id AS STRING) AS product_id,
        quantity,
        list_price,
        discount
    FROM source
)

SELECT * FROM renamed
