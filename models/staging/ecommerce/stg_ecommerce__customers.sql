{{ config(
    materialized='view',
    tags=['staging', 'ecommerce', 'pii']
) }}

with source as (
    select * from {{ source('ecommerce', 'customers') }}
),

renamed as (
    select
        -- Primary key
        id as customer_id,

        -- Customer info
        first_name,
        last_name,
        {{ dbt.concat(['first_name', "' '", 'last_name']) }} as full_name,
        lower(trim(email)) as email,

        -- Timestamps
        created_at as customer_created_at,
        updated_at as customer_updated_at,

        -- Data quality flags
        case 
            when first_name is null or last_name is null then true 
            else false 
        end as is_missing_name_flag,

        case 
            when email is null 
                 or email = '' 
                 or not contains(email, '@') 
            then true 
            else false 
        end as is_invalid_email_flag,

        -- Derived fields
        case 
            when email like '%gmail.com' then 'Gmail'
            when email like '%yahoo.com' then 'Yahoo'
            when email like '%outlook.com' or email like '%hotmail.com' then 'Microsoft'
            else 'Other'
        end as email_provider

    from source
),

final as (
    select * from renamed

    -- Filter out invalid records for data quality
    where customer_id is not null
      and customer_created_at is not null
)

select * from final
