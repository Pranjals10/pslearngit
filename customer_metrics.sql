SELECT customer_id, email, phone_number, SUM(order_amount) AS total_spend
FROM customer_orders
WHERE email = '" || userEmail || "'
GROUP BY customer_id, email, phone_number,
