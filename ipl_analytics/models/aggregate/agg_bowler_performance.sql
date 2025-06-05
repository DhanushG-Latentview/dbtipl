{{ config(
    materialized='incremental',
    unique_key=['bowler_id', 'event_id', 'season', 'match_id']
) }}

with bowler_deliveries as (

    select
        fmd.bowler_id,
        fmd.event_id,
        de.season,
        fmd.match_id,

        count(*) as balls_bowled,

        sum(fmd.runs_batter) as runs_conceded_batter,
        sum(fmd.runs_extras) as runs_conceded_extras,
        sum(fmd.runs_total) as runs_conceded,

        sum(case when fmd.wicket_kind is not null then 1 else 0 end) as wickets,

        sum(case when fmd.is_boundary then 1 else 0 end) as boundaries_conceded,
        sum(case when fmd.runs_batter = 6 then 1 else 0 end) as sixes_conceded,
        sum(case when fmd.is_dot_ball then 1 else 0 end) as dot_balls,

        case when count(*) > 0 then round(sum(fmd.runs_total) * 6.0 / count(*), 2) else null end as economy_rate

    from SNOWFLAKE_LEARNING_DB.dbt_dg.fact_match_deliveries fmd
    left join SNOWFLAKE_LEARNING_DB.dbt_dg.dim_event de on fmd.event_id = de.event_id

    group by
        fmd.bowler_id,
        fmd.event_id,
        de.season,
        fmd.match_id

),

career_agg as (

    select
        bowler_id,
        sum(balls_bowled) as career_balls_bowled,
        sum(runs_conceded) as career_runs_conceded,
        sum(wickets) as career_wickets,
        sum(boundaries_conceded) as career_boundaries_conceded,
        sum(sixes_conceded) as career_sixes_conceded,
        sum(dot_balls) as career_dot_balls,

        case when sum(balls_bowled) > 0
             then round(sum(runs_conceded) * 6.0 / sum(balls_bowled), 2)
             else null
        end as career_economy_rate,

        case when sum(wickets) > 0
             then round(sum(runs_conceded) * 1.0 / sum(wickets), 2)
             else null
        end as career_bowling_average

    from bowler_deliveries

    group by bowler_id

),

season_agg as (

    select
        bowler_id,
        season,
        sum(balls_bowled) as season_balls_bowled,
        sum(runs_conceded) as season_runs_conceded,
        sum(wickets) as season_wickets,
        sum(boundaries_conceded) as season_boundaries_conceded,
        sum(sixes_conceded) as season_sixes_conceded,
        sum(dot_balls) as season_dot_balls,

        case when sum(balls_bowled) > 0
             then round(sum(runs_conceded) * 6.0 / sum(balls_bowled), 2)
             else null
        end as season_economy_rate,

        case when sum(wickets) > 0
             then round(sum(runs_conceded) * 1.0 / sum(wickets), 2)
             else null
        end as season_bowling_average

    from bowler_deliveries

    group by
        bowler_id,
        season

)

select
    bd.bowler_id,
    bd.event_id,
    bd.season,
    bd.match_id,

    bd.balls_bowled,
    bd.runs_conceded_batter,
    bd.runs_conceded_extras,
    bd.runs_conceded,
    bd.wickets,
    bd.boundaries_conceded,
    bd.sixes_conceded,
    bd.dot_balls,
    bd.economy_rate,

    ca.career_balls_bowled,
    ca.career_runs_conceded,
    ca.career_wickets,
    ca.career_boundaries_conceded,
    ca.career_sixes_conceded,
    ca.career_dot_balls,
    ca.career_economy_rate,
    ca.career_bowling_average,

    sa.season_balls_bowled,
    sa.season_runs_conceded,
    sa.season_wickets,
    sa.season_boundaries_conceded,
    sa.season_sixes_conceded,
    sa.season_dot_balls,
    sa.season_economy_rate,
    sa.season_bowling_average

from bowler_deliveries bd
left join career_agg ca on bd.bowler_id = ca.bowler_id
left join season_agg sa on bd.bowler_id = sa.bowler_id and bd.season = sa.season

where concat(bd.bowler_id, '_', bd.event_id, '_', bd.season, '_', bd.match_id) not in (
    select concat(bowler_id, '_', event_id, '_', season, '_', match_id) from SNOWFLAKE_LEARNING_DB.dbt_dg.agg_bowler_performance
)
