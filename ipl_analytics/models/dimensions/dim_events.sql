-- models/dimensions/dim_events.sql

{{ config(
    materialized="incremental",
    unique_key="event_id"
) }}

with source_data as (
    select distinct
        event_name,
        season,
        match_type,
        gender,
        overs_limit,
        team_type,
        balls_per_over
    from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }}
),

generate_key as (
    select
        -- Generate surrogate key using MD5 hash of identifying columns
        {{ dbt_utils.generate_surrogate_key([
            "event_name",
            "season",
            "match_type",
            "gender",
            "overs_limit",
            "team_type",
            "balls_per_over"
        ]) }} as event_id,
        event_name,
        season,
        match_type,
        gender,
        overs_limit,
        team_type,
        balls_per_over
    from source_data
)

select * from generate_key

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where event_id not in (select event_id from {{ this }})

{% endif %}

