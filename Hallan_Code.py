#import
from pyspark.sql import *

#inicializando o spark
spark = SparkSession.builder.master("local").appName("Word_Count")\
    .config("spark.some.config.option", "some-value")\
    .getOrCreate()

#iniciando o dataframe
df = spark.read.option("header", "false") \
    .option("delimiter", ' ') \
    .option("inferSchema", "true") \
    .csv("D:/spark/Texto/access_log_Jul95.txt") \
    .selectExpr("_c0 as Host","_c1 as Col_1","_c2 as Col_2","_c3 as Timestamp","_c4 as Col_3","_c5 as Requisicao","_c6 as Cod_HTTP","_c7 as Bytes")

#Primeira tentariva usando apenas pyspark
#Numero de Hosts unicos:
#_host_count = df.dropDuplicates(['Host']).groupby(df.Col_1).count()
#Numero de Erros 404:
#_404_count = df.filter(df.Cod_HTTP=='404').groupby(df.Col_1).count()

#utilizando o SQL
df.registerTempTable("DETAIL")
_404_count = spark.sql("SELECT COUNT(1) AS ERROS_404 FROM DETAIL WHERE Cod_HTTP='404'")
_host_count = spark.sql("SELECT COUNT(DISTINCT Host) AS HOSTs_UNICOS FROM DETAIL")
_host_count_404 = spark.sql("SELECT Host,count(1) number_of_404 FROM DETAIL WHERE Cod_HTTP='404' GROUP BY Host ORDER BY number_of_404 DESC LIMIT 5")
_404_dia = spark.sql("SELECT Timestamp,count(1) number_of_404 FROM DETAIL where Cod_HTTP='404' group by Timestamp")
_bytes_sum = spark.sql("SELECT cast(sum(Bytes) as decimal (20,2)) sum_bytes FROM DETAIL")

#Mostrar os resultados
_host_count.show(1, truncate=True)
_404_count.show(1, truncate=True)
_host_count_404.show(10, truncate=False)
_404_dia.show(5, truncate=False)
_bytes_sum.show(1, truncate=True)

#Mostrar o DataFrame    
#df.show(15, truncate=True)
