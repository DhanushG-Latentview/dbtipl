with base as (

    select
        -- Unique delivery ID
        md5(
            cast(s.match_id as string) || '_' ||
            cast(s.innings as string) || '_' ||
            cast(s.over_in_innings as string) || '_' ||
            cast(s.delivery_in_over as string)
        ) as delivery_id,

        -- Foreign keys
        e.event_id,
        p_batter.player_id as batter_id,
        p_batter.player_name as batter_name,
        p_bowler.player_id as bowler_id,
        p_bowler.player_name as bowler_name,
        p_non_striker.player_id as non_striker_id,
        p_non_striker.player_name as non_striker_batter_name,
        p_player_of_match.player_id as player_of_match_id,
        t1.team_id as team_1_id,
        t2.team_id as team_2_id,

        -- Match identifiers
        s.match_id,
        s.innings,
        s.over_in_innings,
        s.delivery_in_over,

        -- Raw delivery metrics
        s.runs_batter,
        s.runs_extras,
        s.runs_total,

        -- Derived indicators
        case when s.runs_batter in (4,6) then true else false end as is_boundary,
        case when s.runs_total = 0 then true else false end as is_dot_ball,
        case when s.wicket_kind is not null then true else false end as is_wicket,

        -- Wicket details
        s.wicket_kind,
        p_wicket_player_out.player_id as wicket_player_out_id,
        p_wicket_fielder_1.player_id as wicket_fielder_1_id,
        p_wicket_fielder_2.player_id as wicket_fielder_2_id,

        -- Extras
        s.extras_wides,
        s.extras_noballs,
        s.extras_byes,
        s.extras_legbyes,
        s.extras_penalty,

        -- Review info
        s.review_by,
        s.review_umpire,
        s.review_batter,
        s.review_decision,
        s.review_type,
        s.is_review_umpires_call,

        -- Match context
        s.is_powerplay,
        s.is_super_over,
        s.target_remaining,
        s.balls_remaining

    from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }} s

    left join {{ ref('dim_event') }} e on s.match_id = e.match_id

    left join {{ ref('dim_players') }} p_batter on s.batter = p_batter.player_name
    left join {{ ref('dim_players') }} p_bowler on s.bowler = p_bowler.player_name
    left join {{ ref('dim_players') }} p_non_striker on s.non_striker = p_non_striker.player_name
    left join {{ ref('dim_players') }} p_player_of_match on s.player_of_match = p_player_of_match.player_name

    left join {{ ref('dim_teams') }} t1 on s.team_1 = t1.team_name
    left join {{ ref('dim_teams') }} t2 on s.team_2 = t2.team_name

    left join {{ ref('dim_players') }} p_wicket_player_out on s.wicket_player_out = p_wicket_player_out.player_name
    left join {{ ref('dim_players') }} p_wicket_fielder_1 on s.wicket_fielder_1 = p_wicket_fielder_1.player_name
    left join {{ ref('dim_players') }} p_wicket_fielder_2 on s.wicket_fielder_2 = p_wicket_fielder_2.player_name

)

select * from base
