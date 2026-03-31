SELECT customer.id customer_id
     , web_activity.activity
FROM customer
JOIN web_activity
    ON customer.id = web_activity.customer_id
WHERE customer.id IN ({{ params.customer_ids | map('first') | join(', ') }})