SELECT customer.id
     , lead_score.score
     , cart_activity.activity
FROM customer
INNER JOIN lead_score
    ON customer.id = lead_score.customer_id
LEFT JOIN cart_activity
    ON customer.id = cart_activity.customer_id