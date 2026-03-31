SELECT customer.id
     , web_activity.activity
FROM customer
JOIN web_activity
    ON customer.id = web_activity.customer_id