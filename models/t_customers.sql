{{
    config(
        materialized='incremental',
        unique_key='src_id'
    )
}}

with combined as (

    select *, 'DB1' as src
    from {{ source('public', 'db1_customers') }}
    where _ab_cdc_deleted_at is null

    union all

    select *, 'DB2' as src
    from {{ source('public', 'db2_customers') }}
    where _ab_cdc_deleted_at is null

)

select
    *,
    src || '_' || customerid as src_id
from combined
{% if is_incremental() %}
  where _ab_cdc_updated_at >
      (select max(_ab_cdc_updated_at) from {{ this }})
{% endif %}
