{% macro audit_columns(layer) %}
    {% if layer == 'bronze' %}
        current_timestamp() AS _ingest_at,
        '{{ invocation_id }}' AS _batch_id_bronze
    {% elif layer == 'silver' %}
        current_timestamp() AS _processed_at,
        '{{ invocation_id }}' AS _batch_id_silver
    {% elif layer == 'gold' %}
        current_timestamp() AS _refined_at,
        '{{ invocation_id }}' AS _batch_id_gold
    {% endif %}
{% endmacro %}