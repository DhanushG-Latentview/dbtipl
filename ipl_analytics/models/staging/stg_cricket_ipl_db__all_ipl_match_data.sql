with source as (

    select * from {{ source('cricket_ipl_db', 'all_ipl_match_data') }}

),

renamed as (

    select
        -- Identifiers
        match_id,
        innings,
        over_number as over_in_innings,
        delivery_number as delivery_in_over,

        -- Match Info
        city,
        try_cast(dates_1 as date) as match_date,
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
        winner as match_winner,

        -- Result Info
        match_result,
        by_wickets as won_by_wickets,
        by_runs as won_by_runs,
        method as result_method,
        overs as overs_limit,
        balls_per_over,

        -- Player Info
        batter,
        bowler,
        non_striker,
        player_of_match,

        -- Delivery Details
        runs_batter,
        runs_extras,
        runs_total,
        coalesce(powerplay, false) as is_powerplay,
        super_over as is_super_over,

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
        coalesce(review_umpires_call, false) as is_review_umpires_call,

        -- Target/Remaining
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

        -- Team Player Names
        team_1_player_1, team_1_player_2, team_1_player_3, team_1_player_4, team_1_player_5, team_1_player_6, team_1_player_7, team_1_player_8, team_1_player_9, team_1_player_10, team_1_player_11, team_1_player_12,
        team_2_player_1, team_2_player_2, team_2_player_3, team_2_player_4, team_2_player_5, team_2_player_6, team_2_player_7, team_2_player_8, team_2_player_9, team_2_player_10, team_2_player_11, team_2_player_12,

        -- Team Player IDs
        team_1_player_1_id, team_1_player_2_id, team_1_player_3_id, team_1_player_4_id, team_1_player_5_id, team_1_player_6_id, team_1_player_7_id, team_1_player_8_id, team_1_player_9_id, team_1_player_10_id, team_1_player_11_id, team_1_player_12_id,
        team_2_player_1_id, team_2_player_2_id, team_2_player_3_id, team_2_player_4_id, team_2_player_5_id, team_2_player_6_id, team_2_player_7_id, team_2_player_8_id, team_2_player_9_id, team_2_player_10_id, team_2_player_11_id, team_2_player_12_id,

        -- Replacement Player Info
        replacement_in,
        replacement_out,
        replacement_team,
        replacement_reason

    from source

)

select * from renamed
