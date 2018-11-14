# semantix_code
repository for semantix codes 

Remarks:
* Utilizei apenas o primeiro dataset pois o Segundo não foi possivel realizar o download
* Renomei como access_log_Jul95.txt
* Criei este codigo utilizando o Anaconda, spark-2.4.0-bin-hadoop2.7,pyspark e Jupyter.

Resultados:
+------------+
|HOSTs_UNICOS|
+------------+
|       81983|
+------------+

+---------+
|ERROS_404|
+---------+
|    10832|
+---------+

+---------------------------+-------------+
|Host                       |number_of_404|
+---------------------------+-------------+
|hoohoo.ncsa.uiuc.edu       |251          |
|jbiagioni.npt.nuwc.navy.mil|131          |
|piweba3y.prodigy.com       |109          |
|piweba1y.prodigy.com       |92           |
|phaelon.ksc.nasa.gov       |64           |
+---------------------------+-------------+

+---------------------+-------------+
|Timestamp            |number_of_404|
+---------------------+-------------+
|[01/Jul/1995:08:20:34|1            |
|[02/Jul/1995:01:44:32|1            |
|[02/Jul/1995:06:39:09|1            |
|[03/Jul/1995:07:49:38|1            |
|[03/Jul/1995:17:15:16|1            |
+---------------------+-------------+
only showing top 5 rows

+--------------+
|     sum_bytes|
+--------------+
|38695978742.00|
+--------------+
