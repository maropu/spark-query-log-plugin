[![License](http://img.shields.io/:license-Apache_v2-blue.svg)](https://github.com/maropu/spark-sql-server/blob/master/LICENSE)
[![Build and test](https://github.com/maropu/spark-query-log-plugin/workflows/Build%20and%20test/badge.svg)](https://github.com/maropu/spark-query-log-plugin/actions?query=workflow%3A%22Build+and+test%22)

This is an experimental toolkit to store query logs and provide a way to estimate a similarity between queries.
A query similarity would be useful for real-world usecases such as performance analysis [1] and the sampling of test queries [2].

## Query Logging

To start storing query logs, you need to install this plugin first:

    $ git clone https://github.com/maropu/spark-query-log-plugin.git
    $ cd spark-query-log-plugin.git
    $ ./bin/spark-shell

    scala> import io.github.maropu.spark.QueryLogPlugin
    scala> QueryLogPlugin.install()
    scala> sql("SELECT 1").count()
    scala> val df = QueryLogPlugin.load()
    scala> df.show()
    +-----------------------+-----------------------+------------+---------+-----------------------+
    |              timestamp|                  query| fingerprint| attrRefs|             durationMs|
    +-----------------------+-----------------------+------------+---------+-----------------------+
    |2020-12-04 22:42:52.022|Aggregate [count(1) ...|  -453247703|       {}|{"planning": 1168, "...|
    +-----------------------+-----------------------+------------+---------+-----------------------+

## Grouping based on Query fingerprint

This plugin can convert queries into fingerprints as with [Percona pt-fingerprint](https://www.percona.com/doc/percona-toolkit/LATEST/pt-fingerprint.html).
If a fingerprint value is the same, they are semantically-equal between each other.
So, users can easily analyze query distribution and running time by using a simple aggregation guery as follows:

    scala> sql("SELECT a AS key, SUM(b) AS value FROM (SELECT * FROM VALUES (1, 1) s(a, b)) GROUP BY a").count()
    scala> sql("SELECT COUNT(v) AS v, k AS k FROM VALUES (1, 1) t(k, v) GROUP BY 2").count()
    scala> val df = QueryLogPlugin.load()
    scala> df.selectExpr("query", "fingerprint", "durationMs['execution'] executionMs").write.saveAsTable("ql")
    scala> spark.table("ql").show()
    +-----------------------+-------------+-----------+
    |                  query|  fingerprint|executionMs|
    +-----------------------+-------------+-----------+
    |Aggregate [count(1) ...|  -1883688674|       6094|
    |Aggregate [count(1) ...|  -1883688674|       1498|
    +-----------------------+-------------+-----------+

    scala> sql("SELECT FIRST(query) query, COUNT(1) cnt, AVG(executionMs) FROM ql GROUP BY fingerprint").show()
    +-----------------------+---+----------------+
    |                  query|cnt|avg(executionMs)|
    +-----------------------+---+----------------+
    |Aggregate [count(1) ...|  2|          3796.0|
    +-----------------------+---+----------------+

## Grouping based on Relation References

There are various methods to estimate a similarity between queries depending on use cases.
For example, earlier studies [3,4,5,6] use the references of relational algebra operations (e.g., `selection`, `joins`, and `group-by`)
as a feature vector to compute a query similarity. The "attrRefs" columns in the query log table represents
a list of referenced columns on scan (leaf) operators and the number of times to read these columns.
So, users can group similar queries by using an arbitrary distance function
(the Jaccard similarity coefficient in this example) as follows:

    scala> sql("CREATE TABLE t (a INT, b INT, c INT, d INT)")
    scala> sql("SELECT c, d FROM t WHERE c = 1 AND d = 2").count()
    scala> sql("SELECT lt.d, lt.c, lt.d FROM t lt, t rt WHERE lt.a = rt.a AND lt.b = rt.b AND lt.d = rt.d").count()
    scala> sql("SELECT d, SUM(c) FROM t GROUP BY d HAVING SUM(c) > 10").count()
    scala> val df = QueryLogPlugin.load()
    scala> df.selectExpr("monotonically_increasing_id() rowid", "query", "fingerprint", "map_keys(attrRefs) refs", "durationMs['execution'] executionMs").write.saveAsTable("ql")
    scala> spark.table("ql").show()
    +-----+-------------------------+------------+----------+-----------+
    |rowid|                    query| fingerprint|  attrRefs|executionMs|
    +-----+-------------------------+------------+----------+-----------+
    |    0|Aggregate [count(1) AS...|   291972041|    [d, c]|       3796|
    |    1|Aggregate [count(1) AS...|   560512157| [b, d, a]|       3684|
    |    2|Aggregate [count(1) AS...|  2127616458| [b, a, c]|       2011|
    +-----+-------------------------+------------+----------+-----------+

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
|  spark.sql.queryLog.regularizer.excludedRules |  None  | Configures a list of rules to be disabled in the regularizer, in which the rules are specified by their rule names and separated by comma. |

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
or Twitter([@maropu](http://twitter.com/#!/maropu)).

