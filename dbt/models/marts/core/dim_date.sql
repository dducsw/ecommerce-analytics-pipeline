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
        to_char(date_day, 'Month') as month_name,
        to_char(date_day, 'Mon') as month_name_short,
        to_char(date_day, 'Day') as day_name,
        to_char(date_day, 'Dy') as day_name_short,
        
        -- Formatted Dates
        to_char(date_day, 'YYYY-MM-DD') as date_formatted,
        to_char(date_day, 'YYYY-MM') as year_month,
        to_char(date_day, 'YYYY-Q') as year_quarter,
        concat('W', extract(week from date_day), '-', extract(year from date_day)) as year_week,
        
        -- Date Flags
        case when extract(dayofweek from date_day) in (0, 6) then true else false end as is_weekend,
        case when extract(dayofweek from date_day) between 1 and 5 then true else false end as is_weekday,
        
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
