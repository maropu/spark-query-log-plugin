SELECT COUNT(v) AS v, k AS k FROM VALUES (1, 1) t(k, v) GROUP BY 2;

SELECT a AS key, SUM(b) AS value
FROM (
  SELECT * FROM VALUES (1, 1) s(a, b)
)
GROUP BY a;
