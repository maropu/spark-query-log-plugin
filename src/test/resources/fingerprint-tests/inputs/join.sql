-- Defines test tables
CREATE TABLE ft (a INT) USING parquet;
CREATE TABLE dt1 (b INT) USING parquet;
CREATE TABLE dt2 (c INT) USING parquet;
CREATE TABLE dt3 (d INT) USING parquet;

SELECT * FROM ft, dt1, dt2
WHERE ft.a = dt1.b AND ft.a = dt2.c;

SELECT * FROM ft, dt2, dt1
WHERE ft.a = dt2.c AND ft.a = dt1.b;

SELECT * FROM ft, dt1, dt2, dt3
WHERE ft.a = dt1.b AND ft.a = dt2.c AND ft.a = dt3.d;

SELECT * FROM ft, dt1, dt2, dt3
WHERE ft.a = dt3.d AND ft.a = dt2.c AND ft.a = dt1.b;

SELECT * FROM ft, dt1, dt2, dt3
WHERE ft.a = dt2.c AND ft.a = dt3.d AND ft.a = dt1.b;

SELECT * FROM ft, dt3, dt2, dt1
WHERE ft.a = dt2.c AND ft.a = dt3.d AND ft.a = dt1.b;

SELECT * FROM ft, dt2, dt1, dt3
WHERE ft.a = dt2.c AND ft.a = dt3.d AND ft.a = dt1.b;

SELECT * FROM (
  SELECT a, c FROM ft, dt2 WHERE ft.a = dt2.c
) t, dt1, dt3
WHERE t.a = dt1.b AND t.a = dt3.d;

-- Clean up
DROP TABLE ft;
DROP TABLE dt1;
DROP TABLE dt2;
DROP TABLE dt3;
