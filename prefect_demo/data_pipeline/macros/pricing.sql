
{% macro discounted_amount(extended_price,discount_percentage, scale=2) %}
    (-1*{{extended_price}})::decimal(16, {{ scale }})
{% endmacro %}
