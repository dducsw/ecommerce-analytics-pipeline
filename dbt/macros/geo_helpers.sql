{% macro calculate_distance_km(point1_lon, point1_lat, point2_lon, point2_lat) %}
    (
        6371 * acos(
            cos(acos(-1) * {{ point1_lat }} / 180) * 
            cos(acos(-1) * {{ point2_lat }} / 180) * 
            cos(acos(-1) * {{ point2_lon }} / 180 - acos(-1) * {{ point1_lon }} / 180) + 
            sin(acos(-1) * {{ point1_lat }} / 180) * 
            sin(acos(-1) * {{ point2_lat }} / 180)
        )
    )
{% endmacro %}
