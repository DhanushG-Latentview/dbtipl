-- models/dimensions/dim_players.sql

{{ config(
    materialized="table"
) }}

-- Unpivot player names and IDs from team 1
with team_1_players as (
    select match_id, team_1_player_1_id as player_id, team_1_player_1 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_1_player_1_id is not null
    union all
    select match_id, team_1_player_2_id as player_id, team_1_player_2 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_1_player_2_id is not null
    union all
    select match_id, team_1_player_3_id as player_id, team_1_player_3 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_1_player_3_id is not null
    union all
    select match_id, team_1_player_4_id as player_id, team_1_player_4 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_1_player_4_id is not null
    union all
    select match_id, team_1_player_5_id as player_id, team_1_player_5 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_1_player_5_id is not null
    union all
    select match_id, team_1_player_6_id as player_id, team_1_player_6 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_1_player_6_id is not null
    union all
    select match_id, team_1_player_7_id as player_id, team_1_player_7 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_1_player_7_id is not null
    union all
    select match_id, team_1_player_8_id as player_id, team_1_player_8 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_1_player_8_id is not null
    union all
    select match_id, team_1_player_9_id as player_id, team_1_player_9 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_1_player_9_id is not null
    union all
    select match_id, team_1_player_10_id as player_id, team_1_player_10 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_1_player_10_id is not null
    union all
    select match_id, team_1_player_11_id as player_id, team_1_player_11 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_1_player_11_id is not null
    union all
    select match_id, team_1_player_12_id as player_id, team_1_player_12 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_1_player_12_id is not null
),

-- Unpivot player names and IDs from team 2
team_2_players as (
    select match_id, team_2_player_1_id as player_id, team_2_player_1 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_2_player_1_id is not null
    union all
    select match_id, team_2_player_2_id as player_id, team_2_player_2 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_2_player_2_id is not null
    union all
    select match_id, team_2_player_3_id as player_id, team_2_player_3 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_2_player_3_id is not null
    union all
    select match_id, team_2_player_4_id as player_id, team_2_player_4 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_2_player_4_id is not null
    union all
    select match_id, team_2_player_5_id as player_id, team_2_player_5 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_2_player_5_id is not null
    union all
    select match_id, team_2_player_6_id as player_id, team_2_player_6 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_2_player_6_id is not null
    union all
    select match_id, team_2_player_7_id as player_id, team_2_player_7 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_2_player_7_id is not null
    union all
    select match_id, team_2_player_8_id as player_id, team_2_player_8 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_2_player_8_id is not null
    union all
    select match_id, team_2_player_9_id as player_id, team_2_player_9 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_2_player_9_id is not null
    union all
    select match_id, team_2_player_10_id as player_id, team_2_player_10 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_2_player_10_id is not null
    union all
    select match_id, team_2_player_11_id as player_id, team_2_player_11 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_2_player_11_id is not null
    union all
    select match_id, team_2_player_12_id as player_id, team_2_player_12 as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where team_2_player_12_id is not null
),

-- Combine players from both teams
all_players as (
    select player_id, player_name from team_1_players
    union all
    select player_id, player_name from team_2_players
),

-- Add other involved players (batter, bowler, non-striker, player_of_match, wicket_player_out)
other_players as (
    select player_of_match_id as player_id, player_of_match as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where player_of_match_id is not null
    union all
    select batter as player_id, batter as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where batter is not null -- Assuming batter name is ID for now, needs mapping if not
    union all
    select bowler as player_id, bowler as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where bowler is not null -- Assuming bowler name is ID for now, needs mapping if not
    union all
    select non_striker as player_id, non_striker as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where non_striker is not null -- Assuming non_striker name is ID for now, needs mapping if not
    union all
    select wicket_player_out as player_id, wicket_player_out as player_name from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }} where wicket_player_out is not null -- Assuming wicket_player_out name is ID for now, needs mapping if not
    -- Note: Fielder IDs might need separate handling if they are just names
),

-- Combine all player sources and get distinct players
final_players as (
    select player_id, player_name from all_players
    union all
    select player_id, player_name from other_players
)

-- Select distinct players, ensuring player_id is the primary key
select distinct
    player_id,
    player_name
from final_players
where player_id is not null
order by player_name

