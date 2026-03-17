{% macro audit_columns(layer) %}
    {% if layer == 'bronze' %}
        current_timestamp() as _ingest_at,
        current_date() as _ingest_date,
        '{{ invocation_id }}' as _batch_id_bronze
    {% elif layer == 'silver' %}
        current_timestamp() as _processed_at,
        '{{ invocation_id }}' as _batch_id_silver
    {% elif layer == 'gold' %}
        current_timestamp() as _processed_at_gold,
        '{{ invocation_id }}' as _batch_id_gold
    {% endif %}
{% endmacro %}