{% macro create_point(longitude, latitude) %}
    st_makepoint({{ longitude }}, {{ latitude }})
{% endmacro %}

{% macro calculate_distance_km(point1_lon, point1_lat, point2_lon, point2_lat) %}
    st_distance(
        {{ create_point(point1_lon, point1_lat) }},
        {{ create_point(point2_lon, point2_lat) }}
    ) / 1000
{% endmacro %}
