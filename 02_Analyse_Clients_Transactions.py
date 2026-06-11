# Databricks notebook source
# MAGIC %md
# MAGIC ###Quelques analyse des clients et les trasactions 

# COMMAND ----------

#les dataframes
df_clients = spark.read.table("dev.banque_db.clients")
df_cartes = spark.read.table("dev.banque_db.cards")
df_trasaction = spark.read.table("dev.banque_db.transactions")
df_mcc_code = spark.read.table("dev.banque_db.mcc_codes")
df_user_data = spark.read.table("dev.banque_db.user_data")
df_clients_full = spark.read.table("dev.banque_db.client_full")

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, count
#1-liste de tous les clients avec leur id_client, nom complet et nationalité, salaire,...

list_clients = (
    df_clients_full.alias("cf")
    .join(df_cartes.alias("ca"), col("cf.id") == col("ca.client_id"), "left")
    .groupBy(
        col("cf.id").alias("client_id"),
        concat_ws(" ", col("cf.GivenName"), col("cf.Surname")).alias("nom_complet"),
        col("cf.nationalite"),
        col("cf.yearly_income").alias("salaire")
    )
    .agg(count("ca.id").alias("nb_cartes"))
    .orderBy(col("nb_cartes").desc())
)

list_clients.display()

# COMMAND ----------

#2-Montant total depensé
from pyspark.sql.functions import lit, coalesce, sum
total_depence = (
    df_clients_full.alias("cf")
    .join(df_trasaction.alias("tr"), col("cf.id") == col("tr.client_id"), "left")
    .groupBy(
        col("cf.id").alias("client_id"),
        col("cf.yearly_income").alias("salaire")
    )
    .agg(coalesce(sum(col("tr.amount")), lit(0)).alias("montant_total_transactions"))
    .orderBy(col("montant_total_transactions").desc())
)

total_depence.display()


# COMMAND ----------

#3-classement des client par niveau de dette

client_par_niveau_dette = (
    df_clients_full.alias("cf")
    .selectExpr("id as id_client", "yearly_income as salaire", "total_debt as dette", "round(yearly_income/nullif(total_debt,0),2) as ratio_dette",
    "case when round(yearly_income/nullif(total_debt,0),2) < 0.5 then 'low' when round(yearly_income/nullif(total_debt,0),2) < 1 then 'medium' when round(yearly_income/nullif(total_debt,0),2) < 2 then 'hight' when round(yearly_income/nullif(total_debt,0),2) > 5 then 'ultra_hight' else 'UTP' end as niveau_dette")
    .orderBy(col("ratio_dette").desc())
)

client_par_niveau_dette.display()

# COMMAND ----------

from pyspark.sql.functions import count, sum, col, cast
#4-Transaction par type de secteur

trasac_by_secteur = (
    df_trasaction.alias("tr")
    .join(
        df_mcc_code.alias("mcc"),
        col("tr.mcc").cast("string") == col("mcc.mcc_code"),
        "left"
    )
    .groupBy(
        col("tr.mcc").cast("string"),
        col("tr.use_chip"),
        col("description")
    )
    .agg(
        count(col("tr.id")).alias("nb_transactions"),
        sum(col("tr.amount")).alias("montant_total")
    )
    .selectExpr(
        "mcc",
        "use_chip as type_transaction",
        "description as secteur_description",
        "nb_transactions",
        "montant_total"
    )
    .orderBy(
        col("nb_transactions").desc()
    )
)

trasac_by_secteur.display()

# COMMAND ----------

#5- les carte les plus utilisées

must_use_cards = (
    df_cartes.alias("ca")
    .join(df_trasaction.alias("tr"), col("ca.id") == col("tr.card_id"), "left")
    .groupBy(
        col("ca.id"), col("ca.card_type")
    )
    .agg(
        count(col("tr.id")).alias("nb_transaction"),
        sum(col("tr.amount")).alias("montant_total"),
    )
    .selectExpr("id as card_id", "card_type", "nb_transaction", "montant_total")
    .orderBy(col("nb_transaction").desc())
    .limit(150)
)

must_use_cards.display()

# COMMAND ----------

from pyspark.sql.functions import avg, countDistinct
#6- Client par nationalité

customers_by_nationality = (
    df_clients_full.alias("cf")
    .groupBy(
        col("nationalite")
    )
    .agg(
        countDistinct(col("id")).alias("nb_client"),
        avg(col("cf.yearly_income")).alias("moyenne_salaire"),
        avg(col("cf.total_debt")).alias("moyenne_dette")
    )
)

customers_by_nationality.display()

# COMMAND ----------

#7- Transaction par type de secteur

trasac_by_secteur_type =  (
    df_trasaction.alias("tr")
    .join(
        df_mcc_code.alias("mcc"),
        col("tr.mcc").cast("string") == col("mcc.mcc_code"),
        "left"
    )
    .groupBy(
        col("tr.mcc").cast("string"),
        col("tr.use_chip"),
        col("description")
    )
    .agg(
        count(col("tr.id")).alias("nb_transactions"),
        sum(col("tr.amount")).alias("montant_total")
    )
    .selectExpr(
        "mcc",
        "use_chip as type_transaction",
        "description as secteur_description",
        "nb_transactions",
        "montant_total"
    )
    .orderBy(
        col("nb_transactions").desc()
    )
)

trasac_by_secteur_type.display()

# COMMAND ----------

#8-Clients sans transactions

customer_whitout_transactions = (
    df_clients_full.alias("cf")
    .join(
        df_trasaction.alias("tr"), col("cf.id") == col("tr.client_id"), "left"
    )
    .selectExpr("cf.id as client_id", "concat(GivenName,' ',Surname) as nom_complet", "nationalite", "yearly_income as salaire", "credit_score")
    .where(col("tr.id").isNull())
)

customer_whitout_transactions.display()


# COMMAND ----------

#9- Villes les plus actives avec le plus grand nbre de trasaction

from pyspark.sql import functions as sf
from pyspark.sql import Window

active_cities = (
    df_trasaction.alias("tr")
    .join(
        df_mcc_code.alias("mcc"), col("tr.mcc").cast("string") == col("mcc.mcc_code"), "left"
    )
    .groupBy(
        col("tr.merchant_city")
    )
    .agg(
        count(col("tr.id")).alias("nb_transactions"),
        avg(col("tr.amount")).alias("montant_moyen"),
        sum(col("tr.amount")).alias("montant_total")
    )
    .selectExpr(
        "merchant_city as city",
        "nb_transactions",
        "montant_total",
        "montant_moyen"
    )
    .orderBy(col("nb_transactions").desc())
    .limit(6)
    .withColumn("rank", sf.row_number().over(Window.orderBy(col("nb_transactions").desc())))
)

active_cities.display()


# COMMAND ----------

# 10- Analyse par genre

gender_analysis = (
    df_clients_full.alias("cf")
    .join(
        df_trasaction.alias("tr"), col("cf.id") == col("tr.client_id"), "left"
        )
    .groupBy(
        col("cf.gender")
    )
    .agg(
        count(col("tr.id")).alias("nb_transactions"),
        countDistinct(col("cf.id")).alias("nb_client"),
        avg(col("tr.amount")).alias("montant_moyen"),
        avg(col("cf.total_debt")).alias("moyenne_dette"),
        avg(col("cf.yearly_income")).alias("moyenne_salaire"),
        avg(col("cf.credit_score")).alias("moyenne_credit_score")
    )
    .selectExpr(
        "gender",
        "nb_transactions",
        "nb_client",
        "montant_moyen",
        "moyenne_dette",
        "moyenne_salaire",
        "moyenne_credit_score"
    )
    .orderBy(col("nb_transactions").desc())
)

gender_analysis.display()


# COMMAND ----------

