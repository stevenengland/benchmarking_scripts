SELECT
    o.order_id,
    c.name AS customer,
    e.name AS employee,
    d.dept_name,
    o.order_date,
    SUM(oi.quantity * p.price) AS total_amount
FROM
    orders o
    JOIN customers c ON o.cust_id = c.cust_id
    JOIN employees e ON o.emp_id = e.emp_id
    JOIN departments d ON e.dept_id = d.dept_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.prod_id = p.prod_id
GROUP BY
    o.order_id, c.name, e.name, d.dept_name, o.order_date;