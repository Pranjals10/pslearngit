// Databricks notebook source
// MAGIC %md
// MAGIC # Notebook para la consulta de datos contenidos en el DL
// MAGIC
// MAGIC Este notebook está pensado para obtener los datos necesarios para los tests

// COMMAND ----------

dbutils.widgets.text("table_name", "")
dbutils.widgets.text("query","")


// COMMAND ----------

val table = dbutils.widgets.get("table_name")
val query = dbutils.widgets.get("query")


// COMMAND ----------

// MAGIC %run /Shared/DAB-CRP0012/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_a_nb_arquetipo_functions

// COMMAND ----------

implicit val spark1:SparkSession=spark

// COMMAND ----------

val table_name = getObject(table)

// COMMAND ----------

val df = spark.table(table_name)  
df.createOrReplaceTempView("tabla")

println("\n\n\n FILAS --> ")
println(df.count())
println("\n COLUMNAS --> ")
println(df.columns.size) //se restan las columnas de mes,dia y año
df.printSchema()  
println("QUERY : "+query)


// COMMAND ----------

// MAGIC %md
// MAGIC # Instrucciones para la exprotación
// MAGIC ### CSV:
// MAGIC - La cabecera no se debe de eliminar
// MAGIC - Revisar que los campos de texto no contengan el delimitador, en caso contrario habrá que modificar el fichero para cambiar el delimitador
// MAGIC
// MAGIC ### JSON
// MAGIC - Se debe elimitar la estructrura _fields_
// MAGIC - Se debe quitar el campo _rows_ respetando su valor, una lista con todos los valores del dataset
// MAGIC - Se permiten JSON multilinea

// COMMAND ----------

display(spark.sql(query))