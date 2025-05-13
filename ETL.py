# Databricks notebook source
# MAGIC %md
# MAGIC #Imports
# MAGIC

# COMMAND ----------

import os
import urllib.request
import gzip
import shutil
import tarfile
from pyspark.sql.functions import (
    col, count, countDistinct, sum as _sum, avg, when,
    min as _min, max as _max, datediff, lag, dayofweek, round, to_date
)
from pyspark.sql.window import Window



# COMMAND ----------

# MAGIC %md
# MAGIC #FILES

# COMMAND ----------

# URLs dos arquivos
urls = {
    "order.json.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz",
    "consumer.csv.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/consumer.csv.gz",
    "restaurant.csv.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/restaurant.csv.gz",
    "ab_test_ref.tar.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/ab_test_ref.tar.gz"
}

# Pasta temporária para download
tmp_path = "/tmp"
dbfs_path = "dbfs:/FileStore/tables"


# COMMAND ----------

# MAGIC %md
# MAGIC #Functions

# COMMAND ----------

def extract_csv(dbfs_tar_gz_path, dbfs_csv_output_path):
    local_tar = "/tmp/temp.tar.gz"
    local_extract = "/tmp/extract_csv"

    dbutils.fs.cp(dbfs_tar_gz_path, f"file:{local_tar}", True)
    os.makedirs(local_extract, exist_ok=True)

    with tarfile.open(local_tar, "r:gz") as tar:
        for member in tar.getmembers():
            if member.name.endswith(".csv") and not member.name.startswith("._"):
                tar.extract(member, path=local_extract)
                break  

    csv_file = [f for f in os.listdir(local_extract) if f.endswith(".csv")][0]
    local_csv_path = os.path.join(local_extract, csv_file)
    df = spark.read.csv(f"file:{local_csv_path}", header=True, inferSchema=True)
    df.write.mode("overwrite").option("header", True).csv(dbfs_csv_output_path)

    return df
    
def download_para_dbfs(nome_arquivo, url):
    local_tmp = os.path.join(tmp_path, nome_arquivo)
    urllib.request.urlretrieve(url, local_tmp)
    dbutils.fs.cp(f"file:{local_tmp}", f"{dbfs_path}/{nome_arquivo}")


# COMMAND ----------

# MAGIC %md
# MAGIC # Download Arquivos

# COMMAND ----------

for nome, url in urls.items():
    print(f"Download: {nome}...")
    baixar_para_dbfs(nome, url)

# COMMAND ----------

# MAGIC %md
# MAGIC # Ler arquivos
# MAGIC

# COMMAND ----------

df_consumer = spark.read.csv("dbfs:/FileStore/tables/consumer.csv.gz", header=True, inferSchema=True)
df_restaurant = spark.read.csv("dbfs:/FileStore/tables/restaurant.csv.gz", header=True, inferSchema=True)
df_ab_test = extract_csv("dbfs:/FileStore/tables/ab_test_ref.tar.gz","dbfs:/FileStore/tables/ab_test.ref")
df_order = spark.read.json("dbfs:/FileStore/tables/order.json.gz")


# JOIN da base de pedidos com teste A/B
df = df_order.join(df_ab_test, on="customer_id", how="inner") \
                  .join(df_consumer, on="customer_id", how="left") \
                  .join(df_restaurant, df_order.merchant_id == df_restaurant.id, how="left")



# COMMAND ----------

# MAGIC %md
# MAGIC # Selecionar e tratar colunas

# COMMAND ----------


df = df_order.select("customer_id", "order_id", "order_created_at", "order_total_amount") \
    .join(df_ab_test, on="customer_id", how="inner")

df = df.withColumn("order_date", to_date("order_created_at"))

df = df.filter(col("order_total_amount").isNotNull()) \
       .withColumn("order_created_at", col("order_created_at").cast("timestamp"))



# COMMAND ----------

# MAGIC %md
# MAGIC #Calculo de KPIs:

# COMMAND ----------



## INICIO - KPIs gerais por grupo
df_kpis = df.groupBy("is_target").agg(
    countDistinct("customer_id").alias("usuarios_unicos"),
    count("order_id").alias("total_pedidos"),
    _sum("order_total_amount").alias("gasto_total"),
    (_sum("order_total_amount") / count("order_id")).alias("ticket_medio")  # ticket por pedido
)
## FIM - KPIs gerais por grupo

## INICIO - Métricas por usuário
df_user = df.groupBy("customer_id", "is_target").agg(
    count("order_id").alias("qtd_pedidos"),
    _sum("order_total_amount").alias("gasto_total"),
    _min("order_created_at").alias("primeiro_pedido"),
    _max("order_created_at").alias("ultimo_pedido")
)

# Setar recorrência
df_user = df_user.withColumn("recorrente", (col("qtd_pedidos") > 1).cast("int"))

# Segmentar ticket por usuário
df_user = df_user.withColumn(
    "segmento_ticket",
    when(col("gasto_total") < 25, "baixo")
    .when(col("gasto_total") <= 50, "medio")
    .otherwise("alto")
)
## FIM - Métricas por usuário

## INICIO - Calculo de tempos entre pedidos
window = Window.partitionBy("customer_id").orderBy("order_created_at")

df_intervalo = df.withColumn("dias_desde_anterior", datediff(
    col("order_created_at"),
    lag("order_created_at").over(window)
))

# Remover primeiro pedido pois nao tem pedido anterior para calcular
df_intervalo_user = df_intervalo.filter(col("dias_desde_anterior").isNotNull()) \
    .groupBy("customer_id", "is_target").agg(
        avg("dias_desde_anterior").alias("tempo_medio_entre_pedidos")
    )

# Média do tempo entre pedidos por grupo (target/control)
df_tempo = df_intervalo_user.groupBy("is_target").agg(
    avg("tempo_medio_entre_pedidos").alias("tempo_medio_entre_pedidos_dias")
)
## FIM - Calculo de tempos entre pedidos


## INICIO Métricas agregadas por grupo (recorrência, pedidos, ticket por usuário)
df_recorrencia = df_user.groupBy("is_target").agg(
    avg("recorrente").alias("percentual_recorrentes"),
    avg("qtd_pedidos").alias("pedidos_por_usuario"),
    avg("gasto_total").alias("ticket_medio_por_usuario")
)

# Análise de pedidos final de semana
df_semana = df.withColumn("dia_semana", dayofweek("order_created_at"))

df_semana = df_semana.withColumn(
    "is_fds",
    when(col("dia_semana").isin([6, 7, 1]), 1).otherwise(0)
)

df_fds = df_semana.groupBy("is_target").agg(
    _sum("is_fds").alias("pedidos_fds"),
    round((_sum("is_fds") / count("order_id")) * 100, 2).alias("percentual_fds")
)
## FIM - Métricas agregadas por grupo (recorrência, pedidos, ticket por usuário)

# Consolidar tudo
df_kpis_final = df_kpis \
    .join(df_recorrencia, on="is_target", how="inner") \
    .join(df_tempo, on="is_target", how="inner") \
    .join(df_fds, on="is_target", how="inner")

# COMMAND ----------

display(df_kpis_final)


# COMMAND ----------

