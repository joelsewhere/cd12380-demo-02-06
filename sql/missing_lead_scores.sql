SELECT DISTINCT customer.id
FROM customer
WHERE NOT EXISTS (
  SELECT 1
  FROM lead_score
  WHERE customer.id = lead_score.customer_id
    );