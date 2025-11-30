{{
    config(
        materialized='incremental',
        unique_key='src_id'
    )
}}

with combined as (

    select *, 'DB1' as src
    from {{ source('public', 'db1_orders') }}

    union all

    select *, 'DB2' as src
    from {{ source('public', 'db2_orders') }}

)

select
    *,
    src || '_' || orderid as src_id
from combined
{% if is_incremental() %}
  where _ab_cdc_updated_at >
      (select max(_ab_cdc_updated_at) from {{ this }})
{% endif %}
