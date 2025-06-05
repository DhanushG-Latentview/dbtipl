{{ config(materialized="table") }}

with batter_agg as (
    select
        batter_id as player_id,
        event_id,
        season,
        match_id,

        total_runs,
        balls_faced,
        boundaries,
        sixes,
        dot_balls,
        dismissals,
        strike_rate,

        career_runs,
        career_balls,
        career_boundaries,
        career_sixes,
        career_dot_balls,
        career_dismissals,
        career_strike_rate,

        season_runs,
        season_balls,
        season_boundaries,
        season_sixes,
        season_dot_balls,
        season_dismissals,
        season_strike_rate

    from {{ ref('agg_batter_performance') }}
),

bowler_agg as (
    select
        bowler_id as player_id,
        event_id,
        season,
        match_id,

        balls_bowled,
        runs_conceded_batter,
        runs_conceded_extras,
        runs_conceded,
        wickets,
        boundaries_conceded,
        sixes_conceded,
        dot_balls,
        economy_rate,

        career_balls_bowled,
        career_runs_conceded,
        career_wickets,
        career_boundaries_conceded,
        career_sixes_conceded,
        career_dot_balls,
        career_economy_rate,
        career_bowling_average,

        season_balls_bowled,
        season_runs_conceded,
        season_wickets,
        season_boundaries_conceded,
        season_sixes_conceded,
        season_dot_balls,
        season_economy_rate,
        season_bowling_average

    from {{ ref('agg_bowler_performance') }}
),

combined as (
    select
        coalesce(b.player_id, bw.player_id) as player_id,
        coalesce(b.event_id, bw.event_id) as event_id,
        coalesce(b.season, bw.season) as season,
        coalesce(b.match_id, bw.match_id) as match_id,

        -- Batter stats
        total_runs,
        balls_faced,
        boundaries,
        sixes,
        b.dot_balls as batter_dot_balls,
        dismissals,
        strike_rate,

        career_runs,
        career_balls,
        career_boundaries,
        career_sixes,
        career_dot_balls as career_batter_dot_balls,
        career_dismissals,
        career_strike_rate,

        season_runs,
        season_balls,
        season_boundaries,
        season_sixes,
        season_dot_balls as season_batter_dot_balls,
        season_dismissals,
        season_strike_rate,

        -- Bowler stats
        balls_bowled,
        runs_conceded_batter,
        runs_conceded_extras,
        runs_conceded,
        wickets,
        boundaries_conceded,
        sixes_conceded,
        bw.dot_balls as bowler_dot_balls,
        economy_rate,

        career_balls_bowled,
        career_runs_conceded,
        career_wickets,
        career_boundaries_conceded,
        career_sixes_conceded,
        career_dot_balls as career_bowler_dot_balls,
        career_economy_rate,
        career_bowling_average,

        season_balls_bowled,
        season_runs_conceded,
        season_wickets,
        season_boundaries_conceded,
        season_sixes_conceded,
        season_dot_balls as season_bowler_dot_balls,
        season_economy_rate,
        season_bowling_average

    from batter_agg b
    full outer join bowler_agg bw
    using (player_id, event_id, season, match_id)
),

final as (
    select
        dp.player_name,
        c.*,

        -- Batting derived metrics
        case when total_runs >= 50 then true else false end as is_fifty,
        case when total_runs >= 100 then true else false end as is_century,
        case when dismissals > 0 then total_runs * 1.0 / dismissals else null end as batting_average,
        case when balls_faced > 0 then boundaries * 100.0 / balls_faced else null end as boundary_percentage,

        -- Bowling derived metrics
        case when wickets > 0 then runs_conceded * 1.0 / wickets else null end as bowling_average,
        case when wickets > 0 then balls_bowled * 1.0 / wickets else null end as strike_rate_bowling,
        case when balls_bowled > 0 then bowler_dot_balls * 100.0 / balls_bowled else null end as dot_ball_percentage

    from combined c
    left join {{ ref('dim_players') }} dp
        on c.player_id = dp.player_id
)

select * from final
