{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

with source_data as (

    select distinct
        event_name,
        season,
        match_type,
        gender,
        overs_limit,
        team_type,
        balls_per_over,
        match_id,
        match_number,
        event_stage,
        city,
        venue,
        match_date,
        toss_winner,
        toss_decision,
        match_winner,
        match_result,
        result_method,
        won_by_wickets,
        won_by_runs
    from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}

),

generate_key as (

    select
        {{ dbt_utils.generate_surrogate_key(["match_id"]) }} as event_id,

        event_name,
        season,
        match_type,
        gender,
        overs_limit,
        team_type,
        balls_per_over,
        match_id,
        match_number,
        event_stage,

        case
            when city is null or lower(city) = 'null' then 'Dubai'
            else city
        end as city,

        venue,
        match_date,
        toss_winner,
        toss_decision,
        match_winner,
        match_result,
        result_method,
        won_by_wickets,
        won_by_runs

    from source_data

)


select * from generate_key

{% if is_incremental() %}
  where event_id not in (select event_id from {{ this }})
{% endif %}
