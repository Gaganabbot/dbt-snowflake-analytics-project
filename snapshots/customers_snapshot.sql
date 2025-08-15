{%- snapshot customers_snapshot -%}
    {{
        config(
            target_schema='snapshots',
            unique_key='customer_id',
            strategy='timestamp',
            updated_at='updated_at',
            tags=['snapshot']
        )
    }}

    select * from {{ source('ecommerce', 'customers') }}

{%- endsnapshot -%}
