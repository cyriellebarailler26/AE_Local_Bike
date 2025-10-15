WITH source AS (
    SELECT * FROM {{ source('local_bike', 'customers') }}
),

renamed AS (
    SELECT
        CAST(customer_id AS STRING) AS customer_id,
        INITCAP(first_name) AS first_name,
        INITCAP(last_name) AS last_name,
        LOWER(email) AS email,
        phone,
        street,
        city,
        state,
        zip_code
    FROM source
)

SELECT * FROM renamed
