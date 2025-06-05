{{ config(
    materialized='incremental',
    unique_key=['batter_id', 'event_id', 'season', 'match_id']
) }}

with batter_deliveries as (

    select
        fmd.batter_id,
        fmd.event_id,
        de.season,
        fmd.match_id,

        sum(fmd.runs_batter) as total_runs,
        count(*) as balls_faced,
        sum(case when fmd.is_boundary then 1 else 0 end) as boundaries,
        sum(case when fmd.runs_batter = 6 then 1 else 0 end) as sixes,
        sum(case when fmd.is_dot_ball then 1 else 0 end) as dot_balls,
        sum(fmd.is_wicket::int) as dismissals,

        case when count(*) > 0
             then round(sum(fmd.runs_batter) * 100.0 / count(*), 2)
             else null
        end as strike_rate

    from SNOWFLAKE_LEARNING_DB.dbt_dg.fact_match_deliveries fmd
    left join SNOWFLAKE_LEARNING_DB.dbt_dg.dim_event de on fmd.event_id = de.event_id
    

    group by
        fmd.batter_id,
        fmd.event_id,
        de.season,
        fmd.match_id

),

career_agg as (

    select
        batter_id,
        sum(total_runs) as career_runs,
        sum(balls_faced) as career_balls,
        sum(boundaries) as career_boundaries,
        sum(sixes) as career_sixes,
        sum(dot_balls) as career_dot_balls,
        sum(dismissals) as career_dismissals,

        case when sum(balls_faced) > 0
             then round(sum(total_runs) * 100.0 / sum(balls_faced), 2)
             else null
        end as career_strike_rate

    from batter_deliveries

    group by batter_id

),

season_agg as (

    select
        batter_id,
        season,
        sum(total_runs) as season_runs,
        sum(balls_faced) as season_balls,
        sum(boundaries) as season_boundaries,
        sum(sixes) as season_sixes,
        sum(dot_balls) as season_dot_balls,
        sum(dismissals) as season_dismissals,

        case when sum(balls_faced) > 0
             then round(sum(total_runs) * 100.0 / sum(balls_faced), 2)
             else null
        end as season_strike_rate

    from batter_deliveries

    group by
        batter_id,
        season

)

select
    bd.batter_id,
    bd.event_id,
    bd.season,
    bd.match_id,

    bd.total_runs,
    bd.balls_faced,
    bd.boundaries,
    bd.sixes,
    bd.dot_balls,
    bd.dismissals,
    bd.strike_rate,

    ca.career_runs,
    ca.career_balls,
    ca.career_boundaries,
    ca.career_sixes,
    ca.career_dot_balls,
    ca.career_dismissals,
    ca.career_strike_rate,

    sa.season_runs,
    sa.season_balls,
    sa.season_boundaries,
    sa.season_sixes,
    sa.season_dot_balls,
    sa.season_dismissals,
    sa.season_strike_rate

from batter_deliveries bd
left join career_agg ca on bd.batter_id = ca.batter_id
left join season_agg sa on bd.batter_id = sa.batter_id and bd.season = sa.season

where concat(bd.batter_id, '_', bd.event_id, '_', bd.season, '_', bd.match_id) not in (
    select concat(batter_id, '_', event_id, '_', season, '_', match_id) from SNOWFLAKE_LEARNING_DB.dbt_dg.agg_batter_performance
)
