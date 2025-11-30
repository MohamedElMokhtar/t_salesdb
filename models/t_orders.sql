{{
    config(
        materialized='incremental',
        unique_key=['id', 'src']
    )
}}

with combined as (

    select *, 'DB1' as src
    from {{ source('public', 'db1_orders') }}

    union all

    select *, 'DB2' as src
    from {{ source('public', 'db2_orders') }}

)

select *
from combined
{% if is_incremental() %}
  where _ab_cdc_updated_at >
      (select max(_ab_cdc_updated_at) from {{ this }})
{% endif %}
