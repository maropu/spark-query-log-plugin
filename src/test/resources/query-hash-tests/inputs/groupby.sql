SELECT k AS k, COUNT(v) AS v FROM VALUES (1, 1) t(k, v) GROUP BY k;

SELECT k AS key, COUNT(v) AS v FROM VALUES (1, 1) t(k, v) GROUP BY key;

SELECT k AS k, COUNT(v) AS v FROM VALUES (1, 1) t(v, k) GROUP BY k;

SELECT k, COUNT(v) AS v FROM VALUES (1, 1) t(k, v) GROUP BY k;

SELECT k, SUM(v) AS v FROM VALUES (1, 1) t(k, v) GROUP BY k;

SELECT k, COUNT(v) AS v FROM VALUES (1, 2) t(k, v) GROUP BY k;

SELECT COUNT(v) AS v, k AS k FROM VALUES (1, 1) t(k, v) GROUP BY 2;

SELECT k1, k2, COUNT(v) AS v FROM VALUES (1, 1, 1) t(k1, k2, v) GROUP BY k1, k2;

SELECT k2, k1, COUNT(v) AS v FROM VALUES (1, 1, 1) t(k1, k2, v) GROUP BY k2, k1;

SELECT k1, k2, COUNT(v) AS v FROM VALUES (1, 1, 1) t(k1, k2, v) GROUP BY 2, 1;

SELECT k AS k, COUNT(v) AS value
FROM (
  SELECT * FROM VALUES (1, 1) t(k, v)
)
GROUP BY k;
