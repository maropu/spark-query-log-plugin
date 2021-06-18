[![License](http://img.shields.io/:license-Apache_v2-blue.svg)](https://github.com/maropu/spark-sql-server/blob/master/LICENSE)
[![Build and test](https://github.com/maropu/spark-query-log-plugin/workflows/Build%20and%20test/badge.svg)](https://github.com/maropu/spark-query-log-plugin/actions?query=workflow%3A%22Build+and+test%22)

This is an experimental plugin to store query logs and provide a way to estimate a similarity between queries in your workload.
A query similarity would be useful for real-world usecases such as performance analysis [1] and test query sampling [2].

## Query Logging

To start storing query logs, you need to install this plugin first:

    $ git clone https://github.com/maropu/spark-query-log-plugin.git
    $ cd spark-query-log-plugin.git
    $ ./bin/spark-shell

    scala> import io.github.maropu.spark.QueryLogPlugin
    scala> QueryLogPlugin.install()
    scala> sql("SELECT 1").collect()
    scala> val df = QueryLogPlugin.load()
    scala> df.printSchema()
    root
     |-- timestamp: string (nullable = true)
     |-- query: string (nullable = true)
     |-- semanticHash: integer (nullable = true)
     |-- structuralHash: integer (nullable = true)
     |-- attrRefs: map (nullable = true)
     |    |-- key: string
     |    |-- value: integer (valueContainsNull = true)
     |-- durationMs: map (nullable = true)
     |    |-- key: string
     |    |-- value: long (valueContainsNull = true)

    scala> df.show()
    +--------------------+--------------------+------------+--------------+--------+--------------------+
    |           timestamp|               query|semanticHash|structuralHash|attrRefs|          durationMs|
    +--------------------+--------------------+------------+--------------+--------+--------------------+
    |2021-06-18 15:21:...|Project [1 AS 1#0...| -1944125662|    1920131606|      {}|{execution -> 366...|
    +--------------------+--------------------+------------+--------------+--------+--------------------+

## Grouping based on Query fingerprint

The installed query logger stores query fingerprints as with [Percona pt-fingerprint](https://www.percona.com/doc/percona-toolkit/LATEST/pt-fingerprint.html).
There are the two different types of fingerprints: semantic hash values and structural ones.
If two queries have the same semantic hash value, they are semantically-equal between each other,
e.g., they have the same input relations, grouping columns, and aggregate functions.
On the other hand, if two queries have the same structural hash value,
they have the same structure of an "optimized" plan.
A user can select a preferred fingerprint type for one's use case.
For example, to analyze query distribution and its running time for your workload,
you can run an aggregate query as follows:

    scala> sql("SELECT a, AVG(b) FROM (SELECT * FROM VALUES (1, 1) s(a, b)) GROUP BY a").collect()
    scala> sql("SELECT AVG(v) AS v, k AS k FROM VALUES (1, 1) t(k, v) GROUP BY 2").collect()
    scala> sql("SELECT k AS key, SUM(v) AS value FROM VALUES (1, 1) t(k, v) GROUP BY k").collect()
    scala> val df = QueryLogPlugin.load()
    scala> df.selectExpr("query", "semanticHash", "structuralHash", "durationMs['execution'] executionMs").write.saveAsTable("ql")
    scala> spark.table("ql").show()
    +--------------------+------------+--------------+-----------+
    |               query|semanticHash|structuralHash|executionMs|
    +--------------------+------------+--------------+-----------+
    |Aggregate [a#0], ...|   842308494|   -1629914384|       5612|
    |Aggregate [k#21],...|   842308494|   -1629914384|       1003|
    |Aggregate [k#35],...|  -458077736|   -1629914384|        797|
    +--------------------+------------+--------------+-----------+

    scala> sql("SELECT FIRST(query) query, COUNT(1) cnt, AVG(executionMs) FROM ql GROUP BY semanticHash").show()
    +--------------------+---+----------------+
    |               query|cnt|avg(executionMs)|
    +--------------------+---+----------------+
    |Aggregate [a#0], ...|  2|          3307.5|
    |Aggregate [k#35],...|  1|           797.0|
    +--------------------+---+----------------+

    scala> sql("SELECT FIRST(query) query, COUNT(1) cnt, AVG(executionMs) FROM ql GROUP BY structuralHash").show()
    +--------------------+---+----------------+
    |               query|cnt|avg(executionMs)|
    +--------------------+---+----------------+
    |Aggregate [a#0], ...|  3|          2470.6|
    +--------------------+---+----------------+

## Grouping based on Relation References

There are various methods to estimate a similarity between queries depending on use cases.
For example, earlier studies [3,4,5,6] use the references of relational algebra operations (e.g., `selection`, `joins`, and `group-by`)
as a feature vector to compute a query similarity. The "attrRefs" columns in the query log table represents
a list of referenced columns on scan (leaf) operators and the number of times to read these columns.
So, users can group similar queries by using an arbitrary distance function
(the Jaccard similarity coefficient in this example) as follows:

    scala> sql("CREATE TABLE t (a INT, b INT, c INT, d INT)")
    scala> sql("SELECT c, d FROM t WHERE c = 1 AND d = 2").collect()
    scala> sql("SELECT lt.d, lt.c, lt.d FROM t lt, t rt WHERE lt.a = rt.a AND lt.b = rt.b AND lt.d = rt.d").collect()
    scala> sql("SELECT d, SUM(c) FROM t GROUP BY d HAVING SUM(c) > 10").collect()
    scala> val df = QueryLogPlugin.load()
    scala> df.selectExpr("monotonically_increasing_id() rowid", "query", "structuralHash", "map_keys(attrRefs) refs", "durationMs['execution'] executionMs").write.saveAsTable("ql")
    scala> spark.table("ql").show()
    +-----+--------------------+--------------+--------------------+-----------+
    |rowid|               query|structuralHash|                refs|executionMs|
    +-----+--------------------+--------------+--------------------+-----------+
    |    0|Project [c#2, d#3...|    -469674754|              [d, c]|       1955|
    |    1|Project [d#18, c#...|    -457442679|        [b, d, a, c]|       4078|
    |    2|Project [d#33, su...|    -499160279|              [d, c]|       2105|
    +-----+--------------------+--------------+--------------------+-----------+

    scala> sql("SELECT l.rowid, r.rowid, (size(array_intersect(l.refs, r.refs))) / size(array_distinct(array_union(l.refs, r.refs))) similarity FROM ql l, ql r WHERE l.rowid != r.rowid").show()
    +-----+-----+----------+
    |rowid|rowid|similarity|
    +-----+-----+----------+
    |    0|    1|      0.25|
    |    0|    2|       1.0|
    |    1|    0|      0.25|
    |    1|    2|      0.25|
    |    2|    0|       1.0|
    |    2|    1|      0.25|
    +-----+-----+----------+

## Configurations

|  Property Name  |  Default  |  Meaning  |
| ---- | ---- | ---- |
|  spark.sql.queryLog.randomSamplingRatio  |  1.0  |  Sampling ratio for query logs. |

## References

 - [1] Shaleen Deep and et al., "Comprehensive and Efficient Workload Compression", arXiv, 2020, https://arxiv.org/abs/2011.05549.
 - [2] Jiaqi Yan and et al., "Snowtrail: Testing with Production Queries on a Cloud Database", Proceedings of the Workshop on Testing Database Systems, no.4 pp.1–6, 2018.
 - [3] Kamel Aouiche et al., "Clustering-based materialized view selection in data warehouses", Proceedings of the 10th East European conference on Advances in Databases and Information Systems, pp.81–95. 2006.
 - [4] Julien Aligon et al., "Similarity measures for OLAP sessions", Knowledge and Information Systems volume Knowl 39, pp.463–489, 2014.
 - [5] Vitor Hirota Makiyama et al., "Text Mining Applied to SQL Queries: A Case Study for the SDSS SkyServer", SIMBig, 2015.
 - [6] Gokhan Kul et al., "Similarity Metrics for SQL Query Clustering", IEEE Transactions on Knowledge and Data Engineering, vol.30, no.12, pp.2408-2420, 2018.

## TODO

 - Implements various connectors (e.g., PostgreSQL and MySQL) to store query logs
 - Supports more features to estimate query similarities
 - Add rules for plan regularization (See [6])

## Bug reports

If you hit some bugs and requests, please leave some comments on [Issues](https://github.com/maropu/spark-query-log-plugin/issues)
or Twitter ([@maropu](http://twitter.com/#!/maropu)).

