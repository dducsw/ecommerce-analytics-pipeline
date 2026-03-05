{{ config(materialized='table') }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2018-01-01' as date)",
        end_date="cast('2027-12-31' as date)"
    ) }}
),

date_dimension as (
    select
        date_day,
        
        -- Date Parts
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        extract(week from date_day) as week_of_year,
        extract(day from date_day) as day_of_month,
        extract(dayofweek from date_day) as day_of_week,
        extract(dayofyear from date_day) as day_of_year,
        
        -- Date Names
        format_date('%B', date_day) as month_name,
        format_date('%b', date_day) as month_name_short,
        format_date('%A', date_day) as day_name,
        format_date('%a', date_day) as day_name_short,
        
        -- Formatted Dates
        format_date('%Y-%m-%d', date_day) as date_formatted,
        format_date('%Y-%m', date_day) as year_month,
        concat(format_date('%Y', date_day), '-Q', cast(extract(quarter from date_day) as string)) as year_quarter,
        concat('W', cast(extract(week from date_day) as string), '-', cast(extract(year from date_day) as string)) as year_week,
        
        -- Date Flags (BigQuery: 1=Sunday, 7=Saturday)
        case when extract(dayofweek from date_day) in (1, 7) then true else false end as is_weekend,
        case when extract(dayofweek from date_day) between 2 and 6 then true else false end as is_weekday,
        
        -- Fiscal Calendar (assuming fiscal year starts in January)
        case 
            when extract(month from date_day) >= 1 then extract(year from date_day)
            else extract(year from date_day) - 1
        end as fiscal_year,
        
        case 
            when extract(month from date_day) in (1, 2, 3) then 1
            when extract(month from date_day) in (4, 5, 6) then 2
            when extract(month from date_day) in (7, 8, 9) then 3
            else 4
        end as fiscal_quarter

    from date_spine
)

select * from date_dimension
