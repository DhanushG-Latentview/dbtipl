-- models/intermediate/int_player_batting_match_aggregates.sql

with deliveries as (
    select * from {{ ref("fct_deliveries") }}
),

dates as (
    select * from {{ ref("dim_dates") }}
),

matches as (
    select * from {{ ref("dim_matches") }}
),

players as (
    select * from {{ ref("dim_players") }}
),

match_batting_stats as (
    select
        d.match_id,
        d.batting_team_id,
        d.batter_id,
        m.match_date_key,
        
        sum(d.runs_batter) as total_runs,
        count(*) as balls_faced,
        sum(case when d.runs_batter = 4 then 1 else 0 end) as fours,
        sum(case when d.runs_batter = 6 then 1 else 0 end) as sixes,
        max(case when d.is_wicket = true and d.wicket_player_out_id = d.batter_id then 1 else 0 end) as is_out -- Flag if the batter got out in this match
        
    from deliveries d
    join matches m on d.match_id = m.match_id
    where d.batter_id is not null
    group by 1, 2, 3, 4
)

select 
    mbs.match_id,
    mbs.match_date_key,
    mbs.batting_team_id,
    mbs.batter_id,
    p.player_name as batter_name,
    mbs.total_runs,
    mbs.balls_faced,
    mbs.fours,
    mbs.sixes,
    mbs.is_out,
    -- Calculate strike rate, handle division by zero
    case 
        when mbs.balls_faced > 0 then round((mbs.total_runs * 100.0) / mbs.balls_faced, 2)
        else 0 
    end as strike_rate,
    -- Flags for milestones (can be refined)
    case when mbs.total_runs >= 100 then true else false end as scored_hundred,
    case when mbs.total_runs >= 50 and mbs.total_runs < 100 then true else false end as scored_fifty

from match_batting_stats mbs
left join players p on mbs.batter_id = p.player_id

