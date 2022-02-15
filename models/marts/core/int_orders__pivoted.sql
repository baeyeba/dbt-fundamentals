{% set status_values = ['returned','completed','return_pending','shipped','placed'] -%}
with orders as (
    select * from {{ ref('stg_orders')}} 
),
pivoted as (
    select customer_id,
        {%- for status in status_values %}
            sum(case when status = '{{ status }}' then 1 else 0 end) as {{status}}_aantal
            {%- if not loop.last -%}
                ,
            {%- endif %}
        {%- endfor -%}
    from orders
    group by customer_id
    order by customer_id
)
select * from pivoted