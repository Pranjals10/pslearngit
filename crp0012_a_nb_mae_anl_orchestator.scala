// Databricks notebook source
// MAGIC %md
// MAGIC %md
// MAGIC # Plantilla Notebook para desarrollo a medida
// MAGIC
// MAGIC #### Objetivo
// MAGIC Este notebook una plantilla para implementar un desarrollo a medida
// MAGIC
// MAGIC #### Uso
// MAGIC Para su hay que definir los siguiente parámetros desde la pipeline que se invoque
// MAGIC
// MAGIC | Parámetro | Valor por defecto | Descripcion |
// MAGIC |---|---|---|
// MAGIC |applicationName||Nombre de la aplicación|
// MAGIC |uuid||Identificador único de la ejecución|
// MAGIC |puid||Identificador único del padre|
// MAGIC |name||Nombre para el trazado|
// MAGIC |step| 0 |Paso en la ejecución de la pipeline|
// MAGIC |uniqueKey| |Identificador de la ejecución|
// MAGIC |referenceDate||Fecha de ejecución del orquestador|
// MAGIC |digitalCase| |Código del caso digital|
// MAGIC |optimizeDelta|false |Indica si se ejecuta _vacuum_ y _optimize_ en la tabla delta|
// MAGIC |sources||<table class="GeneratedTable">  <thead>    <tr>      <th>Parámetro</th>      <th>Descripción</th>    </tr>  </thead>  <tbody>    <tr>          </tr>    <tr>         </tr>    <tr>      <td>source_object</td>      <td>Ruta en la que se encuentran los datos</td>    </tr>    <tr>      <td>source_format</td>      <td>Formato del objeto a leer</td>    </tr>  <tr>      <td>source_format_options</td>      <td>Opciones del formato</td>    </tr> <tr>      <td>source_alias</td>      <td>Identificador del dataset</td>    </tr> </tbody></table>|
// MAGIC |target||<table class="GeneratedTable">  <thead>    <tr>      <th>Parámetro</th>      <th>Descripción</th>    </tr>  </thead>  <tbody>    <tr>        </tr>    <tr>        </tr>    <tr>      <td>target_object</td>      <td>Un objeto en el que se almacenan los datos resultantes</td>    </tr>    <tr>      <td>target_format</td>      <td>Formato de la ruta a escribir</td>    </tr>   <tr> <td>target_dl_name_tags</td> <td>Si se utiliza el modo de sobreescritura dinámica, proporcione los nombres de las columnas que se deben usar para determinar qué particiones se deben sobrescribir</td> </tr> <tr> <td>target_write_mode</td> <td>Especifica el tipo de operación de escritura que se debe realizar en la tabla de destino (por ejemplo: overwrite, dynamic, scd1)</td> </tr> <td>target_operation_name</td>      <td>la operación objetivo que debe realizarse utilizando writeWrapper</td>    </tr> </tbody></table>|
// MAGIC |name||Nombre para el trazado|
// MAGIC
// MAGIC #### Nota
// MAGIC
// MAGIC En las rutas en formato delta se permite evolución de esquema.
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC Inicializar widgets y recuperar parámetros

// COMMAND ----------

dbutils.widgets.text("params", "")
val params: String = dbutils.widgets.get("params")

dbutils.widgets.text("sources", "")
val sources = dbutils.widgets.get("sources")

dbutils.widgets.text("target", "")
val target = dbutils.widgets.get("target")

dbutils.widgets.text("name", "")
val name: String = dbutils.widgets.get("name")

dbutils.widgets.text("sparkProperties", "")
val spark_properties: String = dbutils.widgets.get("sparkProperties")

dbutils.widgets.text("auxParams"," ")
val auxParamsStr = dbutils.widgets.get("auxParams")

dbutils.widgets.text("applicationName", "")
val applicationName: String = dbutils.widgets.get("applicationName")

dbutils.widgets.text("uuid", "N/A")
val uuid: String = dbutils.widgets.get("uuid")

dbutils.widgets.text("parentUid", "N/A")
val puid: String = dbutils.widgets.get("parentUid")

dbutils.widgets.text("referenceDate", "")
val referenceDate: String = dbutils.widgets.get("referenceDate")

dbutils.widgets.text("uniqueKey", "")
val uniqueKey: String = dbutils.widgets.get("uniqueKey")

dbutils.widgets.text("digitalCase", "CRP0012")
val digitalCase: String = dbutils.widgets.get("digitalCase")

dbutils.widgets.text("optimizeDelta", "false")
val optimizeDelta: Boolean = dbutils.widgets.get("optimizeDelta").toBoolean

// COMMAND ----------

// MAGIC %run /Shared/DAB-CRP0012/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_a_nb_arquetipo_functions

// COMMAND ----------

implicit val spark1:SparkSession=spark

// COMMAND ----------

// MAGIC %md
// MAGIC Analizar parámetros JSON y extraer información de origen

// COMMAND ----------

val mapper = new ObjectMapper() with ClassTagExtensions
mapper.registerModule(DefaultScalaModule)
mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

val sourcesFinal: List[Map[String, Any]] = mapper.readValue[List[Map[String, Any]]](sources)
val targetFinal: Map[String, Any] = mapper.readValue[Map[String, Any]](target)

val auxParams: Map[String, String] = mapper.readValue[Map[String, String]](auxParamsStr)


val itservices = auxParams("itservices")
val mdc1 = auxParams("mdc1")
val mdc2 = auxParams("mdc2")
val mdc3 = auxParams("mdc3")
val segc = auxParams("segc")
val visint = auxParams("visint")

val json1 = auxParams("json1")
val json2 = auxParams("json2")

val source_format = sourcesFinal.head("source_format").toString
val source_alias = sourcesFinal.head("source_alias").toString
val sourceNumPartions = sourcesFinal.head("source_num_partitions").toString
val source_object = sourcesFinal.head("source_object").toString
val source_deep_partition = sourcesFinal.head("source_deep_partition").toString

val target_format = targetFinal("target_format").toString
val target_dl_name_tags = Option(targetFinal.get("target_dl_name_tags")).map(_.toString).getOrElse("").split(",").map(_.trim).toList
val target_write_mode = Option(targetFinal.getOrElse("target_write_mode", "")).map(_.toString).getOrElse("")
val target_object = targetFinal("target_object").toString
val target_operation_name=targetFinal("target_operation_name").toString

var error:String = ""

// COMMAND ----------

implicit val uid_app = (uuid, puid, applicationName)

if (digitalCase.trim.nonEmpty) {
    ControlHelper().setCustomDigitalCase(digitalCase)
}

LogHelper().logStart()

println(s"applicationName = $applicationName")
println(s"uuid = $uuid")
println(s"puid = $puid")
println(s"uniqueKey = $uniqueKey")
println(s"sources = $sources")
println(s"target = $target")
println(s"auxParams = $auxParams")
println(s"params = $params")
println(s"name = $name")
println(s"optimizeDelta = $optimizeDelta")

// COMMAND ----------

// MAGIC %md
// MAGIC Configurar Spark a partir de propiedades JSON

// COMMAND ----------

var spark_spark_properties = spark_properties.replaceAll("[\t]", "")
val withCommas = spark_spark_properties.replaceAll("\"\\s*\"", "\", \"")

// Convert string to DataFrame
val df = Seq(withCommas).toDF("json_col")

// Define schema
val schema = MapType(StringType, StringType)
 
// Extract JSON into Map
val jsonMap = df.select(from_json($"json_col", schema).as("map"))
                .as[Map[String, String]]
                .head()

jsonMap.foreach { case (key, value) =>
  spark.conf.set(key, value)
  println(f"$key : ${spark.conf.get(key)}")
}



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

import scala.collection.JavaConverters._
implicit val formats = DefaultFormats

val Parsedjson1 = parse(readFile(s"$json1")).extract[List[Map[String, String]]]

val rows = Parsedjson1.map(map => Row(Option(map.getOrElse("deboEjecutarme", "1")).map(_.toLong).orNull, map("tabla"), map("periodicidad")))

// Define the schema
val schema = StructType(List(
  StructField("deboEjecutarme", LongType,nullable = true),
  StructField("tabla", StringType, nullable = false),
  StructField("periodicidad", StringType, nullable = false)
))

// Create the DataFrame
val dfDatabaseInventory = spark.createDataFrame(rows.asJava, schema)

// var Parsedjson2 = readConfiguration[List[Map[String, String]]](s"$json2")
val Parsedjson2 = parse(readFile(s"$json2")).extract[List[Map[String, String]]]
val rows1 = Parsedjson2.map(map => Row(map("pbi"), map("tabla")))


// Define the schema
val schema1 = StructType(List(
  StructField("pbi", StringType, nullable = false),
  StructField("tabla", StringType, nullable = false)
))

// Create the DataFrame
val dfOrchestrator = spark.createDataFrame(rows1.asJava, schema1).withColumnRenamed("tabla", "tabla2")

// COMMAND ----------

var dfInventory = dfDatabaseInventory.join(dfOrchestrator, dfDatabaseInventory("tabla")===dfOrchestrator("tabla2"), "inner")
                                    .select("pbi", "tabla", "periodicidad", "deboEjecutarme")
                                    .na.fill(1,Seq("deboEjecutarme"))
                                    .withColumn("deboEjecutarme", col("deboEjecutarme").cast("Int"))
                                    .withColumn("pbi",when(col("pbi").equalTo("itservices"),itservices)
                                                        .when(col("pbi").equalTo("segc"),segc)
                                                        .when(col("pbi").equalTo("mdc1"),mdc1)
                                                        .when(col("pbi").equalTo("mdc2"),mdc2)
                                                        .when(col("pbi").equalTo("mdc3"),mdc3)
                                                        .when(col("pbi").equalTo("visint"),visint)
                                    )
display(dfInventory)

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

display(dfInventory)

// COMMAND ----------

// MAGIC %md
// MAGIC # Monitorización (Pbis lanzados y tablas origen ejecutadas)

// COMMAND ----------

// DBTITLE 1,Filter and Prepare DataFrame for Execution Check
var dataframeMonitor=getDataFromSource(sourcesFinal(0))
                .filter(to_date($"startDate")=== current_date()) 
                .select($"definition",$"result",$"extraInfo1",$"startDate",$"endDate",$"definition.datasets.datasetOut" as "dataset")


// dataframeMonitor.printSchema
dataframeMonitor.cache
// display(dataframeMonitor)

val pbiejecutados=dataframeMonitor
        .filter($"definition.component"==="crp0012_a_pl_orchestator" and $"result"==="OK")
        .select($"definition.application").as[String].distinct.collect.toList

// //se prepara df para comprobar si se puede ejecutar
dataframeMonitor=dataframeMonitor
                    .withColumn("result",when(($"result").isNull, lit("")).otherwise($"result"))
                    .filter(($"definition.component").isin("crp0012_a_pl_loadData") and  ($"result"=!="KO") )
                    .dropDuplicates("dataset","result")

display(dataframeMonitor)

println(pbiejecutados)

// COMMAND ----------

// MAGIC %md
// MAGIC Filtrar y combinar dataframes para el análisis de inventario

// COMMAND ----------

val mergeDF=dfInventory.join(
        dataframeMonitor,dfInventory("tabla")=== dataframeMonitor("dataset"),"left")
        .withColumn("result",when(($"result").isNull, lit("")).otherwise($"result"))
        .filter(($"ejecucion"===1 and ($"result"=!="OK"))  //si es obligatorio y no ha acabado ok
                or ($"ejecucion"===2 and $"startDate".isNotNull and ($"result"=!="OK")) ) //si es opcional, lanzado y no terminado

mergeDF.cache
display(mergeDF)


// COMMAND ----------

// MAGIC %md
// MAGIC Verifique las condiciones de lanzamiento y actualice las listas de estado

// COMMAND ----------

import scala.collection.mutable.ListBuffer
var pending = new ListBuffer[String]()
var running = new ListBuffer[String]()
var launched = new ListBuffer[String]()
var na = new ListBuffer[String]()

def comprobacionLanzamiento(pbi:String, mergeDF: DataFrame): Unit = {
    if (!pbiejecutados.contains(pbi)){ 
        //se buscan los que no han hecho join, y que frenan la ejecución del informe
        val resultdf=mergeDF.withColumn("result",when(($"result").isNull, lit("")).otherwise($"result"))
                            .filter($"pbi"===pbi)
        display(resultdf)

        val count=resultdf.count()

        if(count>0){
            println("No se cumplen las condiciones de lanzamiento: "+count)
            pending+=pbi
        } else {
            println("Se cumplen las condiciones de lanzamiento")
            running+=pbi
        }
    } else { //ya ha sido lanzado, no se hace nada
         println("Proceso ya lanzado")
         launched+=pbi
    }
}

// COMMAND ----------

// MAGIC %md
// MAGIC # IT SERVICES (diario)

// COMMAND ----------

// MAGIC %md
// MAGIC Verifique las condiciones de lanzamiento y actualice las listas de estado

// COMMAND ----------

comprobacionLanzamiento(itservices, mergeDF)

// COMMAND ----------

// MAGIC %md
// MAGIC # SEGUIMIENTO DE COSTES (desde 7 hasta 13)

// COMMAND ----------

println(day)
println(segc)
if (day>=7 && day<=13){
    comprobacionLanzamiento(segc, mergeDF)
} else {
    na+=segc
}


// COMMAND ----------

// MAGIC %md
// MAGIC # MODELO DE COSTES 1 (diaria)

// COMMAND ----------

comprobacionLanzamiento(mdc1, mergeDF)

// COMMAND ----------

// MAGIC %md
// MAGIC # MODELO DE COSTES 2 (desde 12 hasta 25)

// COMMAND ----------

// if (boollast15days){
if (day>=12 && day<=25){
    comprobacionLanzamiento(mdc2, mergeDF)
} else {
    na+=mdc2
}

// COMMAND ----------

// MAGIC %md
// MAGIC # MODELO DE COSTES 3 (Diario)

// COMMAND ----------

comprobacionLanzamiento(mdc3, mergeDF)

// COMMAND ----------

// MAGIC %md
// MAGIC # VISIÓN INTERNA CLIENTES(desde 7 hasta 13)

// COMMAND ----------

// DBTITLE 1,Check Day Range and Execute `comprobacionLanzamiento`
if ((day>=7 && day<=13) || day==3){
    comprobacionLanzamiento(visint, mergeDF)
} else {
    na+=visint
}

// COMMAND ----------

// MAGIC %md
// MAGIC Crear DataFrame a partir de los datos de estado y mostrar

// COMMAND ----------

// Your data
val data = Seq(
  ("PENDING", pending.toList.mkString(",")),
  ("RUNNING", running.toList.mkString(",")),
  ("LAUNCHED", launched.toList.mkString(",")),
  ("N_A", na.toList.mkString(","))
)

// Schema definition
val schema = StructType(Array(
  StructField("status", StringType, true),
  StructField("values", StringType, true)
))

// Convert to Row and then to Java list
val rowData: java.util.List[Row] = data.map(Row.fromTuple).asJava

// Create DataFrame from Java list
val df = spark.createDataFrame(rowData, schema)

// Display the DataFrame
display(df)


// COMMAND ----------

try{
writeWrapper(df, conf= targetFinal, optimize = optimizeDelta, operationName= target_operation_name,prefix="target")
}
catch {
  case e:Exception => println(e.getMessage)
  error=e.getMessage
}

// COMMAND ----------


val result = "df_orchestrator"

if (!error.isEmpty) {
    LogHelper().logError(s"$name: $error")
    throw new Exception(s"$name: $error")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  