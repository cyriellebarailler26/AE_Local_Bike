WITH source AS (
    SELECT * FROM {{ source('local_bike', 'staffs') }}
),

renamed AS (
    SELECT
        CAST(staff_id AS STRING) AS staff_id,
        INITCAP(first_name) AS first_name,
        INITCAP(last_name) AS last_name,
        LOWER(email) AS email,
        phone,
        active,
        CAST(store_id AS STRING) AS store_id,
        CAST(manager_id AS STRING) AS manager_id
    FROM source
)

SELECT * FROM renamed
