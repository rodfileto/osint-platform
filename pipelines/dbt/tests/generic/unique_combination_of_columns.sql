{% test unique_combination_of_columns(model, combination_of_columns) %}
    {%- set columns_list = combination_of_columns | join(', ') %}
    select {{ columns_list }}
    from {{ model }}
    group by {{ columns_list }}
    having count(*) > 1
{% endtest %}
