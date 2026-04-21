{% test warn_if_unknown(model, column_name) %}

    {{ config(severity = 'warn') }}

    select *
    from {{model}}
    where {{column_name}}='unknown'

{% endtest%}