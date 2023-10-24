WITH

-- import CTEs
orders as (
    select *
    from {{ ref('stg_jaffle_shop_orders') }}
),

customers as (
    select * 
    from {{ ref('stg_jaffle_shop_customers') }}
),

paid_orders as (
    select *
    from {{ ref('int_orders') }}
),

-- logical CTEs

customer_orders as (
    select 
        customers.customer_id, 
        min(order_placed_at) as first_order_date, 
        max(order_placed_at) as most_recent_order_date, 
        count(orders.order_id) as number_of_orders

    from customers
    left join orders
    on orders.customer_id = customers.customer_id
    group by 1
),

x as(

    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad

    from paid_orders p

    left join paid_orders t2 
    on p.customer_id = t2.customer_id and p.order_id >= t2.order_id

    group by 1
    order by p.order_id

),

final as (
    select
        paid_orders.*,
        row_number() over (order by paid_orders.order_id) as transaction_seq,
        row_number() over (partition by paid_orders.customer_id order by paid_orders.order_id) as customer_sales_seq,
        case 
            when customer_orders.first_order_date = paid_orders.order_placed_at
            then 'new'
            else 'return' 
        end as nvsr,
        x.clv_bad as customer_lifetime_value,
        customer_orders.first_order_date as fdos

    from paid_orders

    left join customer_orders using (customer_id)
    left outer join x

    on x.order_id = paid_orders.order_id
    order by order_id
)

-- simple select statement
select * from final
