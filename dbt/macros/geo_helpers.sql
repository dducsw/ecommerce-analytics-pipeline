{% macro calculate_distance_km(point1_lon, point1_lat, point2_lon, point2_lat) %}
    (
        6371 * acos(
            cos(radians({{ point1_lat }})) * 
            cos(radians({{ point2_lat }})) * 
            cos(radians({{ point2_lon }}) - radians({{ point1_lon }})) + 
            sin(radians({{ point1_lat }})) * 
            sin(radians({{ point2_lat }}))
        )
    )
{% endmacro %}
