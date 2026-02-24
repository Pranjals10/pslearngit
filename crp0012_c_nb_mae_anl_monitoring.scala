// Databricks notebook source
// MAGIC %run /Shared/DAB-CRP0012/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_a_nb_arquetipo_functions

// COMMAND ----------

implicit val spark1:SparkSession=spark

// COMMAND ----------

// MAGIC %md
// MAGIC # **Creación de variables con Fechas**

// COMMAND ----------

import java.util.Date
import java.time.{ZoneId, LocalDateTime}

var date = new Date();
var localDateTime = date.toInstant().atZone(ZoneId.of("Europe/Madrid")).toLocalDateTime();
var day = localDateTime.getDayOfMonth();
var hour = localDateTime.getHour()
var localDate = localDateTime.toLocalDate
var lastDayofMonth = localDate.withDayOfMonth(localDate.getMonth().length(localDate.isLeapYear())).getDayOfMonth()
//last dayofMonth siempre va a ser mayor que 15, así que puedo restar directamente
var boolweekdays = localDate.getDayOfWeek.getValue<6
var boollast15days = day>(lastDayofMonth-15)

// COMMAND ----------

// MAGIC %md
// MAGIC # **DATASET: databaseInventory**

// COMMAND ----------

// DBTITLE 1,reading json from volume by using readfile function.
val json = readFile("config_crp0012/config_files/json/common/crp0012_c_cfg_databaseInventory.json")

var dfInventory = spark.read.option("mergeSchema",true).json(Seq(json).toDS()).drop("deboEjecutarme")

// COMMAND ----------

display(dfInventory)

// COMMAND ----------

// MAGIC %md
// MAGIC # DatabaseInventory: ejecuciones

// COMMAND ----------

//Transformación DF DatabaseInventory: marcado de ejecución obligatoria/opcional/nula
dfInventory = dfInventory.withColumn("ejecucion", 
        when(trim($"periodicidad")==="Diario" 
             or (trim($"periodicidad")==="Diario (L-V)"  and lit(boolweekdays))
             or (trim($"periodicidad").like("%Últimos 15 días mes%") and lit(boollast15days))
             or (trim($"periodicidad").like("%Mensual (desde 7 hasta 12)%") and lit(day)>=7 and lit(day)<=12)
             /*NEW*/
             or (trim($"periodicidad").like("%Mensual (desde 12 hasta 25)%") and lit(day)>=12 and lit(day)<=25)
             or (trim($"periodicidad").like("%Mensual (desde 28 hasta 6)%") and lit(day)>=28 and lit(day)<=6)
             /*END*/
             or (trim($"periodicidad").like("%Mensual (7 y 12)%") and (lit(day)===7 or lit(day)===12))
             or (trim($"periodicidad").like("%Mensual (3 y 7)%") and (lit(day)===3 or lit(day)===7))
             or (trim($"periodicidad").like("%Mensual día 7%") and lit(day)===7), 1).otherwise(0)
        ).withColumn("ejecucion", when(
                ($"ejecucion")===0 and lower(trim($"periodicidad")).like("%a demanda%"),2
                ).otherwise($"ejecucion"))
//las que se tienen que ejecutar (1) o a demanda, en el dia de hoy, tienen que ser obtenidas
dfInventory=dfInventory.filter($"ejecucion"=!=0)
// display(dfInventory)

// COMMAND ----------

var table_name="datahub01%env%aucdatagold.log.crp0012_tb_log_control_table"
table_name = getObject(table_name)

// Spark 3.3 to 3.4
spark.conf.set("spark.sql.optimizer.runtime.bloomFilter.enabled", "false")
spark.conf.set("spark.sql.session.timeZone", "Europe/Madrid")

val dataframeMonitor = spark.read.table(table_name)
  .filter(to_date($"startDate") === current_date())
  .select($"definition", $"result", $"extraInfo1", $"startDate", $"endDate", $"definition.datasets.datasetOut" as "dataset")
  .withColumn("dataset", $"definition.datasets.datasetOut")


// dataframeMonitor.printSchema
dataframeMonitor.cache


// Se sacan los dataframes que estan en progreso o en OK
var dfMonitor = dataframeMonitor
                    .withColumn("result",when(($"result").isNull, lit("")).otherwise($"result"))
                    .filter(
                          $"definition.component" === "crp0012_a_pl_loadData" &&
                          $"result" =!= "KO"
                        )
                    .dropDuplicates("dataset","result")

// Se sacan aquellos que dan KO
var dfKO = dataframeMonitor
    .withColumn("result",when(($"result").isNull, lit("")).otherwise($"result"))
    .filter(
        $"definition.component" === "crp0012_a_pl_loadData" &&
        $"result" === "KO"
      )
    .dropDuplicates("dataset","result")

display(dfKO)
display(dfMonitor)

// COMMAND ----------

val dfFailedOrNotExecuted = dfInventory.join(dfMonitor,dfInventory("tabla")=== dfMonitor("dataset"),"left_anti")
                            .filter(($"ejecucion"===1))
val dfFailed = dfFailedOrNotExecuted.join(dfKO,dfFailedOrNotExecuted("tabla")=== dfKO("dataset"),"inner")
val dfNotExecuted = dfFailedOrNotExecuted.as("d1").join(dfFailed.as("d2"),($"d1.tabla" === $"d2.tabla"),"left_anti")

// display(dfFailedOrNotExecuted)
//display(dfFailed)
display(dfNotExecuted)


// COMMAND ----------

// MAGIC %md
// MAGIC # Salida Tabla: Periodicidad:

// COMMAND ----------

val notExecutedList = "Not Executed Tables: " + 
  dfNotExecuted.selectExpr("concat('Table: ', tabla, ', Periodicidad: ', periodicidad) as combined")
    .as[String]
    .collect()
    .mkString("; ")

val failedList = "Failed Tables: " + 
  dfFailed.selectExpr("concat('Table: ', tabla, ', Periodicidad: ', periodicidad) as combined")
    .as[String]
    .collect()
    .mkString("; ")

val reportList = s"$notExecutedList, $failedList"

dbutils.notebook.exit(reportList)
