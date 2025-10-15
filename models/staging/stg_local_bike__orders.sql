WITH source AS (
    SELECT * FROM {{ source('local_bike', 'orders') }}
),

renamed AS (
    SELECT
        CAST(order_id AS STRING) AS order_id,
        CAST(customer_id AS STRING) AS customer_id,
        order_status,
        order_date,
        required_date,
        shipped_date,
        CAST(store_id AS STRING) AS store_id,
        CAST(staff_id AS STRING) AS staff_id
    FROM source
)

SELECT * FROM renamed
