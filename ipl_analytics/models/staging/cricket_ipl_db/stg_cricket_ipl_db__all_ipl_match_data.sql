-- models/staging/cricket_ipl_db/stg_cricket_ipl_db__all_ipl_match_data.sql

with source as (

    select * from {{ source("cricket_ipl_db", "all_ipl_match_data") }}

),

renamed as (

    select
        -- Identifiers
        match_id,
        innings,
        over_number as over_in_innings, -- Renamed to avoid clash with reserved word
        delivery_number as delivery_in_over, -- Renamed for clarity

        -- Match Info
        city,
        try_cast(dates_1 as date) as match_date, -- Assuming dates_1 is the primary match date
        -- dates_2, dates_3, dates_4, dates_5, dates_6, -- Keep if needed, maybe parse later
        event_name,
        match_number,
        event_stage,
        gender,
        match_type,
        match_type_number,
        season,
        team_type,
        venue,

        -- Team Info
        team_1,
        team_2,
        toss_winner,
        toss_decision,
        winner as match_winner, -- Renamed for clarity

        -- Result Info
        match_result,
        by_wickets as won_by_wickets, -- Renamed for clarity
        by_runs as won_by_runs, -- Renamed for clarity
        method as result_method, -- Renamed for clarity
        overs as overs_limit, -- Renamed for clarity
        balls_per_over,

        -- Player Info (Batting/Bowling/Fielding)
        batter,
        bowler,
        non_striker,
        player_of_match,

        -- Delivery Details
        runs_batter,
        runs_extras,
        runs_total,
        coalesce(powerplay, false) as is_powerplay, -- Handle potential NULLs
        super_over as is_super_over, -- Assuming this is boolean or similar

        -- Wicket Details
        wicket_kind,
        wicket_player_out,
        wicket_fielder_1,
        wicket_fielder_2,

        -- Extras Details
        extras_wides,
        extras_noballs,
        extras_byes,
        extras_legbyes,
        extras_penalty,

        -- Review Details
        review_by,
        review_umpire,
        review_batter,
        review_decision,
        review_type,
        coalesce(review_umpires_call, false) as is_review_umpires_call, -- Handle potential NULLs

        -- Target/Remaining (Potentially useful for live analysis, less for historical)
        target_remaining,
        balls_remaining,

        -- Officials (Names)
        match_referees,
        reserve_umpires,
        tv_umpires,
        umpire_1,
        umpire_2,

        -- Officials (IDs)
        match_referees_id,
        reserve_umpires_id,
        tv_umpires_id,
        umpire_1_id,
        umpire_2_id,

        -- Player of Match ID
        player_of_match_id,

        -- Team Player Names (will be unpivoted later for dim_players)
        team_1_player_1, team_1_player_2, team_1_player_3, team_1_player_4, team_1_player_5, team_1_player_6, team_1_player_7, team_1_player_8, team_1_player_9, team_1_player_10, team_1_player_11, team_1_player_12,
        team_2_player_1, team_2_player_2, team_2_player_3, team_2_player_4, team_2_player_5, team_2_player_6, team_2_player_7, team_2_player_8, team_2_player_9, team_2_player_10, team_2_player_11, team_2_player_12,

        -- Team Player IDs (will be unpivoted later for dim_players)
        team_1_player_1_id, team_1_player_2_id, team_1_player_3_id, team_1_player_4_id, team_1_player_5_id, team_1_player_6_id, team_1_player_7_id, team_1_player_8_id, team_1_player_9_id, team_1_player_10_id, team_1_player_11_id, team_1_player_12_id,
        team_2_player_1_id, team_2_player_2_id, team_2_player_3_id, team_2_player_4_id, team_2_player_5_id, team_2_player_6_id, team_2_player_7_id, team_2_player_8_id, team_2_player_9_id, team_2_player_10_id, team_2_player_11_id, team_2_player_12_id,

        -- Replacement Player Info (Handle if needed)
        replacement_in,
        replacement_out,
        replacement_team,
        replacement_reason

        -- Note: Assuming source table has one row per delivery, but also contains match-level info repeated.
        -- Dimensions will deduplicate match-level info.

    from source

)

select * from renamed

