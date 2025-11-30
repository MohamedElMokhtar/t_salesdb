{{
    config(
        materialized='incremental',
        unique_key='src_id'
    )
}}

with combined as (

    select *, 'AHS' as src
    from {{ source('public', 'ahs_factures') }}

    union all

    select *, 'CSM' as src
    from {{ source('public', 'csm_factures') }}

)

select
    *,
    src || '_' || customerid as src_id
from combined
{% if is_incremental() %}
  where _ab_cdc_updated_at >
      (select max(_ab_cdc_updated_at) from {{ this }})
{% endif %}
