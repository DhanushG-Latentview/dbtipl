with raw_teams as (

    select distinct team_1 as team_name
    from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}
    
    union
    
    select distinct team_2 as team_name
    from {{ ref('stg_cricket_ipl_db__all_ipl_match_data') }}

),

cleaned_teams as (

    select
        team_name
    from raw_teams
    where team_name is not null

),

generate_ids as (

    select
        md5(team_name) as team_id,
        team_name
    from cleaned_teams

)

select *
from generate_ids
