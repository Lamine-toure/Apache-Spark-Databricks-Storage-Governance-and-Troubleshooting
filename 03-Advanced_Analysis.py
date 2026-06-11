# Databricks notebook source
# MAGIC %md
# MAGIC ###Dans cette deuxième partie, on fera une analyse avancé

# COMMAND ----------

#les dataframes
df_clients = spark.read.table("dev.banque_db.clients")
df_cartes = spark.read.table("dev.banque_db.cards")
df_trasaction = spark.read.table("dev.banque_db.transactions")
df_mcc_code = spark.read.table("dev.banque_db.mcc_codes")
df_user_data = spark.read.table("dev.banque_db.user_data")
df_clients_full = spark.read.table("dev.banque_db.client_full")

# COMMAND ----------

#1- Evolution du comportement de dépense par client
from pyspark.sql.functions import (quarter, year, count, round, abs, when, avg, col)

trimestriels = (
    df_trasaction
    .withColumn("trimestre", quarter(col("date")))
    .withColumn("years",year(col("date")))
    .groupBy(col("client_id"), col("trimestre"), col("years")
    )
    .agg( avg("amount").alias("montant_moyen"), count("*").alias("nb_transactions")
    )
)

t1 = (
    trimestriels.filter((col("trimestre") == 1) & (col("nb_transactions") >= 5))
    .select("client_id", "montant_moyen", "nb_transactions", "years")
)

t4 = (
    trimestriels.filter((col("trimestre") == 4) & (col("nb_transactions") >= 5))
    .select("client_id", "montant_moyen", "nb_transactions", "years")
)

resultat = (
    t1.alias("t1").join(t4.alias("t4"),((col("t1.client_id") == col("t4.client_id")) & (col("t1.years") == col("t4.years"))),"inner")
    .select(
        col("t1.client_id"), col("t1.years"), 
        round(col("t1.montant_moyen"), 2).alias("montant_moyen_qfirst"),
        round(col("t4.montant_moyen"), 2).alias("montant_moyen_qlast"),

        round(((col("t4.montant_moyen") - col("t1.montant_moyen")) / when(abs(col("t1.montant_moyen")) != 0,
                                                                          abs(col("t1.montant_moyen")))) * 100, 2).alias("variation_pct"))
    .orderBy(col("variation_pct").desc())
)

resultat.display()

# COMMAND ----------

#2- Détection d'anomalies de transactions