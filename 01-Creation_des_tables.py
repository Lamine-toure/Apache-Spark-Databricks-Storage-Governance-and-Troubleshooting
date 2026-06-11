# Databricks notebook source
# MAGIC %md
# MAGIC ####Creation de Catalog, Database et le Volume

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS dev.banque_db;
# MAGIC CREATE VOLUME IF NOT EXISTS dev.banque_db.datasets;

# COMMAND ----------

# MAGIC %md
# MAGIC #####Creation des Tables et insersion de données

# COMMAND ----------

#table mcc_code
import json

with open("/Volumes/dev/banque_db/datasets/Data/mcc_codes.json") as f:
    mcc_dict = json.load(f)

#transformons en liste de tuples
mcc_list = [(k,v) for k,v in mcc_dict.items()]

#creation du dataframe
df_mcc = spark.createDataFrame(mcc_list, ["mcc_code", "description"])

df_mcc.write.mode("overwrite").saveAsTable("dev.banque_db.mcc_codes")

#df_mcc.display()

# COMMAND ----------

#tables clients
df_clients_raw = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", "True")
    .load("/Volumes/dev/banque_db/datasets/Data/users_data.csv")
)

#df_clients_raw.display()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

#traitement
df_clients = (
    df_clients_raw
    .withColumn("id", col("id").cast("string"))
    .withColumn("per_capita_income", regexp_replace("per_capita_income", "\\$", "").cast("double"))
    .withColumn("yearly_income", regexp_replace("yearly_income", "\\$", "").cast("double"))
    .withColumn("total_debt", regexp_replace("total_debt", "\\$", "").cast("double"))
)

(
    df_clients.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dev.banque_db.clients")
)

#df_clients.display()

# COMMAND ----------

#table cartes
df_carts_raw = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", "True")
    .load("/Volumes/dev/banque_db/datasets/Data/cards_data.csv")
)

# COMMAND ----------

from pyspark.sql.functions import to_date
#triatement
df_carts =(
    df_carts_raw
    .withColumn("id", col("id").cast("string"))
    .withColumn("client_id", col("client_id").cast("string"))
    .withColumn("credit_limit", regexp_replace("credit_limit", "\\$", ""))
    .withColumn("card_on_dark_web", col("card_on_dark_web").cast("boolean"))
    .withColumn("has_chip", col("has_chip").cast("boolean"))
    .withColumn("acct_open_date", to_date(col("acct_open_date"), "MM/yyyy"))
)

#Sauvegarde dans une table
df_carts.write.mode("overwrite").saveAsTable("dev.banque_db.cards")

# COMMAND ----------

#table transaction
trasaction_raw = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", "True")
    .load("/Volumes/dev/banque_db/datasets/Data/transactions_data.csv")
)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

trasaction = (
    trasaction_raw
    .withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ssZ"))
    .withColumn("amount", regexp_replace("amount", "\\$", ""))
    .withColumn("card_id", col("card_id").cast("string"))
    .withColumn("id", col("id").cast("string"))
    .withColumn("client_id", col("client_id").cast("string"))
)

trasaction.write.mode("overwrite").saveAsTable("dev.banque_db.transactions")


# COMMAND ----------

#la tableau user_ext
user_ext_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/dev/banque_db/datasets/Data/users_ext.csv")
)
#transformation
user_ext = (
    user_ext_raw
    .withColumn("client_id", col("client_id").cast("string"))
)

user_ext.write.mode("overwrite").saveAsTable("dev.banque_db.user_ext")

# COMMAND ----------

#table user
user_data_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/dev/banque_db/datasets/Data/users_data.csv")
)

#rtransformation
user_data = (
    user_data_raw
    .withColumn("id", col("id").cast("string"))
    .withColumn("per_capita_income", regexp_replace("per_capita_income", "\\$", "").cast("double"))
    .withColumn("yearly_income", regexp_replace("yearly_income", "\\$", "").cast("double"))
    .withColumn("total_debt", regexp_replace("total_debt", "\\$", "").cast("double"))
)

user_data.write.mode("overwrite").saveAsTable("dev.banque_db.user_data")

# COMMAND ----------

#creation d'une vue client_full
spark.sql("""
          create or replace view dev.banque_db.client_full as 
          select c.*, u_ext.* EXCEPT(u_ext.Gender, u_ext.Latitude, u_ext.Longitude)
          from dev.banque_db.clients as c
          left join dev.banque_db.user_ext as u_ext
          on c.id = u_ext.client_id
          """)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.banque_db.client_full

# COMMAND ----------

