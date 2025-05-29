-- models/dimensions/dim_dates.sql

with date_spine as (
    -- Generate a date spine from the earliest to latest match date in the source data
    -- This requires a date utility package or manual SQL generation depending on the warehouse
    -- Using a CTE for demonstration; replace with warehouse-specific function if available (e.g., Snowflake's DATE_SPINE)
    select 
        cast(d as date) as full_date
    from (
        select 
            min(match_date) as start_date,
            max(match_date) as end_date
        from {{ ref("stg_cricket_ipl_db__all_ipl_match_data") }}
    ) dates,
    -- Simple date generation CTE (replace with warehouse function if possible)
    unnest(generate_series(dates.start_date::timestamp, dates.end_date::timestamp, interval '1 day')) as d
),

date_attributes as (
    select
        full_date,
        extract(year from full_date) as year,
        extract(month from full_date) as month_number,
        extract(day from full_date) as day_of_month,
        extract(dayofweek from full_date) as day_of_week_number, -- Sunday=0 or 1 depending on warehouse
        extract(dayofyear from full_date) as day_of_year,
        extract(weekofyear from full_date) as week_of_year,
        extract(quarter from full_date) as quarter_of_year,
        
        -- Day Name
        case extract(dayofweek from full_date)
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as day_name,

        -- Month Name
        case extract(month from full_date)
            when 1 then 'January'
            when 2 then 'February'
            when 3 then 'March'
            when 4 then 'April'
            when 5 then 'May'
            when 6 then 'June'
            when 7 then 'July'
            when 8 then 'August'
            when 9 then 'September'
            when 10 then 'October'
            when 11 then 'November'
            when 12 then 'December'
        end as month_name

    from date_spine
)

select 
    to_number(to_char(full_date, 'YYYYMMDD')) as date_key, -- Surrogate key in YYYYMMDD format
    *
from date_attributes
order by full_date

