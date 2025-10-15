WITH source AS (
    SELECT * FROM {{ source('local_bike', 'stores') }}
),

renamed AS (
    SELECT
        CAST(store_id AS STRING) AS store_id,
        INITCAP(store_name) AS store_name,
        phone,
        email,
        street,
        city,
        state,
        zip_code
    FROM source
)

SELECT * FROM renamed
