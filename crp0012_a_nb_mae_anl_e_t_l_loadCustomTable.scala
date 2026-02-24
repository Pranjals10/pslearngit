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
// MAGIC |target||<table class="GeneratedTable">  <thead>    <tr>      <th>Parámetro</th>      <th>Descripción</th>    </tr>  </thead>  <tbody>    <tr>        </tr>    <tr>        </tr>    <tr>      <td>target_object</td>      <td>Un objeto en el que se almacenan los datos resultantes</td>    </tr>    <tr>      <td>target_format</td>      <td>Formato de la ruta a escribir</td>    </tr>   <td>target_operation_name</td>      <td>la operación objetivo que debe realizarse utilizando writeWrapper</td>    </tr> </tbody></table>|
// MAGIC |name||Nombre para el trazado|
// MAGIC
// MAGIC #### Nota
// MAGIC
// MAGIC En las rutas en formato delta se permite evolución de esquema .
// MAGIC

// COMMAND ----------

// DBTITLE 1,Create Widget
dbutils.widgets.text("applicationName", "")
val applicationName: String = dbutils.widgets.get("applicationName")

dbutils.widgets.text("uuid", "N/A")
val uuid: String = dbutils.widgets.get("uuid")

dbutils.widgets.text("parentUid", "N/A")
val puid: String = dbutils.widgets.get("parentUid")

dbutils.widgets.text("optimizeDelta", "false")
val optimizeDelta: Boolean = dbutils.widgets.get("optimizeDelta").toBoolean

dbutils.widgets.text("uniqueKey", "")
val uniqueKey: String = dbutils.widgets.get("uniqueKey")

dbutils.widgets.text("digitalCase", "CRP0012")
val digitalCase: String = dbutils.widgets.get("digitalCase")

dbutils.widgets.text("referenceDate", "")
val referenceDate: String = dbutils.widgets.get("referenceDate")

dbutils.widgets.text("params", "")
val params: String = dbutils.widgets.get("params")

dbutils.widgets.text("auxParams", "")
val auxParams = dbutils.widgets.get("auxParams")


dbutils.widgets.text("sources", " ")
val sources = dbutils.widgets.get("sources")


dbutils.widgets.text("target", "")
val target = dbutils.widgets.get("target")


dbutils.widgets.text("name", "")
val name: String = dbutils.widgets.get("name")


dbutils.widgets.text("sparkProperties", "")
val spark_properties: String = dbutils.widgets.get("sparkProperties")


// COMMAND ----------

// MAGIC %run /Shared/DAB-CRP0012/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_a_nb_arquetipo_functions

// COMMAND ----------

implicit val spark1:SparkSession=spark

// COMMAND ----------

val mapper = new ObjectMapper() with ClassTagExtensions

mapper.registerModule(DefaultScalaModule)
mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

var sourcesFinal: List[Map[String, Any]] = mapper.readValue[List[Map[String, Any]]](sources)
var targetFinal: Map[String, Any] = mapper.readValue[Map[String, Any]](target)
val paramsFinal: Map[String, Any] = if (params == null || params.trim.isEmpty) Map.empty[String, Any] else mapper.readValue[Map[String, Any]](auxParams)
val auxParamsFinal: Map[String, Any] = mapper.readValue[Map[String, Any]](auxParams)
val target_object = getObject(targetFinal("target_object").toString)
var error:String = ""
implicit val myDigitalCase = digitalCase

// COMMAND ----------

// DBTITLE 1,Load Parameters and Spark Config
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

implicit val uid_app = (uuid, puid, applicationName)
 
if (digitalCase.trim.nonEmpty) {
    print(digitalCase)
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

// DBTITLE 1,Set Env in Target
val Target_object=getObject(target_object)

// COMMAND ----------

// DBTITLE 1,: Access json1 from auxParams
val json1 =auxParamsFinal("json1").toString

// COMMAND ----------

// DBTITLE 1,Load JSON into Spark DataFrame

val json = readFile(json1)

var newDf = spark.read.option("mergeSchema",true).json(Seq(json).toDS()).withColumn("txt_pi_reference", col("txt_pi_reference").cast(StringType)).distinct
newDf.cache

// COMMAND ----------

display(newDf)
newDf.count()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Guardado en EDW

// COMMAND ----------

// DBTITLE 1,Write DataFrame into Target
println(s"Writing $target_object")
try
{
writeWrapper(
  sourceDF = newDf,
  conf = targetFinal,
  optimize = false,
  prefix = "target",
  operationName = targetFinal("target_operation_name").toString
)
}
catch {
  case e:Exception => println(e.getMessage)
  error=e.getMessage
}

// COMMAND ----------

val result = "This is for origen  pipeline"
 
if (!error.isEmpty) {
    LogHelper().logError(s"$name: $error")
    throw new Exception(s"$name: $error")
}
 
LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)