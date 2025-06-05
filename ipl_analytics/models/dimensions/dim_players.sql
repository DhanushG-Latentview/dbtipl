with raw_players as (

    -- Pull all player-related names from the staging model
    select distinct batter as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct bowler as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct non_striker as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct player_of_match as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct wicket_player_out as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct wicket_fielder_1 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct wicket_fielder_2 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct review_batter as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}

    -- Team players from both teams
    union select distinct team_1_player_1 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_1_player_2 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_1_player_3 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_1_player_4 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_1_player_5 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_1_player_6 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_1_player_7 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_1_player_8 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_1_player_9 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_1_player_10 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_1_player_11 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_1_player_12 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_2_player_1 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_2_player_2 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_2_player_3 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_2_player_4 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_2_player_5 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_2_player_6 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_2_player_7 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_2_player_8 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_2_player_9 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_2_player_10 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_2_player_11 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    union select distinct team_2_player_12 as player_name from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}

),

cleaned_players as (

    select distinct
        player_name
    from raw_players
    where player_name is not null

),

generate_ids as (

    select
        md5(player_name) as player_id,
        player_name
    from cleaned_players

)

select *
from generate_ids
