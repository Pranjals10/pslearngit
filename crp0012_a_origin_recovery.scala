// Databricks notebook source
dbutils.widgets.text("applicationName", "")
val applicationName: String = dbutils.widgets.get("applicationName")

dbutils.widgets.text("uuid", "N/A")
val uuid: String = dbutils.widgets.get("uuid")

dbutils.widgets.text("parentUid", "N/A")
val puid: String = dbutils.widgets.get("parentUid")

dbutils.widgets.text("step", "0")
val step: Int = dbutils.widgets.get("step").toInt

dbutils.widgets.text("uniqueKey", "")
val uniqueKey: String = dbutils.widgets.get("uniqueKey")

dbutils.widgets.text("digitalCase", "CRP0012")
val digitalCase: String = dbutils.widgets.get("digitalCase")

dbutils.widgets.text("optimizeDelta", "false")
val optimizeDelta: Boolean = dbutils.widgets.get("optimizeDelta").toBoolean

dbutils.widgets.text("params", "")
val params: String = dbutils.widgets.get("params")

dbutils.widgets.text("auxParams", "")
val auxParams = dbutils.widgets.get("auxParams")

dbutils.widgets.text("sources", "")
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

//mapper.readValue[Map[String, Any]](params).get("sources")
// val paramsFinal: Map[String, Any] = if (params == null || params.trim.isEmpty) Map.empty[String, Any] else mapper.readValue[Map[String, Any]](params)
//paramsFinal.get("sources").get.asInstanceOf[List[Map[String,Any]]]
//mapper.readValue[List[Map[String, Any]]](paramsFinal.get("sources").get)

// COMMAND ----------

val mapper = new ObjectMapper() with ClassTagExtensions

mapper.registerModule(DefaultScalaModule)
mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

val paramsFinal: Map[String, Any] = Option(params)
  .filter(_.trim.nonEmpty)
  .map(json => mapper.readValue[Map[String, Any]](json))
  .getOrElse(Map.empty[String, Any])
val sourcesFinal: List[Map[String, Any]] = paramsFinal
  .get("sources")
  .map(_.asInstanceOf[List[Map[String, Any]]])
  .getOrElse(mapper.readValue[List[Map[String, Any]]](sources))

val targetFinal: Map[String, Any] = paramsFinal
  .get("target")
  .map(_.asInstanceOf[Map[String, Any]])
  .getOrElse(mapper.readValue[Map[String, Any]](target))

val aux_paramsFinal: Map[String, Any] = paramsFinal
  .get("aux_params")
  .map(_.asInstanceOf[Map[String, Any]])
  .getOrElse(mapper.readValue[Map[String, Any]](auxParams))
var error:String = ""

// COMMAND ----------

// DBTITLE 1,Replace function for replaceQueryYearMonthDay
def replaceQueryYearMonthDay(DLSourcePath: Row, query: String, incremental: Boolean): String = {
  var newQuery = query

  if (incremental) {
    // Extract year, month, and day from Row
    if (DLSourcePath.length >= 1)
      newQuery = newQuery.replaceFirst("year +as +num_year", DLSourcePath.get(0).toString + " as num_year")

    if (DLSourcePath.length >= 2)
      newQuery = newQuery.replaceFirst("month +as +num_month", DLSourcePath.get(1).toString + " as num_month")

    if (DLSourcePath.length >= 3)
      newQuery = newQuery.replaceFirst("day +as +num_day", DLSourcePath.get(2).toString + " as num_day")
  }

  newQuery
}


// COMMAND ----------

def extractValuesDynamically(filterOpt: Option[String]): Option[Row] = {
  filterOpt.map { filterStr =>
    // Regex to find all values inside single quotes after '='
    val valuePattern: Regex = """=\s*'([^']+)'""".r

    // Extract all matches in order
    val values = valuePattern.findAllMatchIn(filterStr).map(_.group(1)).toArray

    // Convert to Int if possible, else keep as String
    val convertedValues = values.map(v => 
      try { v.toInt } catch { case _: Throwable => v }
    )

    Row.fromSeq(convertedValues)
  }
}


// COMMAND ----------


def applyDDLFromTableToDF(df: DataFrame, tableName: String)(implicit spark: SparkSession): DataFrame = {
  // Step 1: Extract raw DDL
  val ddlString = spark.sql(s"SHOW CREATE TABLE $tableName")
    .collect()
    .map(_.getString(0))
    .mkString("\n")

  println(s"Raw DDL:\n$ddlString")

  // Step 2: Extract the full balanced parentheses block for columns
  val pattern = """\((?s)(.*)\)\s*USING""".r
  val columnBlock = pattern.findFirstMatchIn(ddlString).map(_.group(1))
    .getOrElse(throw new RuntimeException("Could not extract columns section from DDL."))

  // Step 3: Remove COMMENTs safely and keep entire lines including complex types
  val cleanedColumnDDL = columnBlock
    .split("(?<=\\)),?\\s*\n") // split on line endings or end of column lines (preserving DECIMAL(x,y))
    .map(_.replaceAll("COMMENT\\s+'.*?'", "").trim) // remove COMMENTs
    .filter(_.nonEmpty)
    .mkString(", ")

  println(s"Cleaned column DDL:\n$cleanedColumnDDL")

  // Step 4: Parse to target schema
  val targetSchema = StructType.fromDDL(cleanedColumnDDL)

  // Step 5: Align DataFrame columns
  val dfColsMap = df.columns.map(c => c.toLowerCase -> c).toMap

  val alignedCols = targetSchema.fields.map { field =>
    val inputCol = dfColsMap.getOrElse(field.name.toLowerCase,
      throw new IllegalArgumentException(s"Column '${field.name}' not found in input DataFrame.")
    )
    df(inputCol).cast(field.dataType).as(field.name)
  }

  df.select(alignedCols: _*)
}


 

// COMMAND ----------

// DBTITLE 1,Logging Activity

implicit val uid_app = (uuid, puid, applicationName)

if (digitalCase.trim.nonEmpty) {
    print(digitalCase)
    ControlHelper().setCustomDigitalCase(digitalCase)
}

LogHelper().logStart()

println(s"applicationName = $applicationName")
println(s"uuid = $uuid")
println(s"puid = $puid")
println(s"step = $step")
println(s"uniqueKey = $uniqueKey")
println(s"sources = $sources")
println(s"target = $target")
println(s"name = $name")

// COMMAND ----------

var incremental: Boolean = aux_paramsFinal("incremental").toString.toBoolean
val sql_file:String = aux_paramsFinal("sql_file").toString
val keycol_str = aux_paramsFinal.get("keycolumns")

// COMMAND ----------

var source_num_partition = sourcesFinal.head("source_num_partitions").toString.toInt
var source_deep_partition = sourcesFinal.head("source_deep_partition").toString.toInt
var source_object = sourcesFinal.head("source_object").toString
var target_object=targetFinal("target_object").toString
val target_operation_name=targetFinal("target_operation_name").toString
val target_write_mode=targetFinal("target_write_mode").toString

// COMMAND ----------

// MAGIC %md
// MAGIC

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

val keycolumns: Seq[String] = keycol_str match {
  case Some(cols: List[_]) => cols.map(_.toString)
  case _ => Seq.empty[String]
}

// COMMAND ----------

// DBTITLE 1,read the query
var query = readFile(s"config_crp0012/config_files/sql/origen/$sql_file")

// COMMAND ----------

if (! incremental) {
val updatedSourcesFinal = sourcesFinal.map { sourceMap =>
  sourceMap
    .updated("source_num_partition", 0)
    .updated("source_deep_partition", 0)
}
sourcesFinal = updatedSourcesFinal
source_num_partition = sourcesFinal.head("source_num_partition").toString.toInt
source_deep_partition = sourcesFinal.head("source_deep_partition").toString.toInt
}


// COMMAND ----------


val source_object = getObject(sourcesFinal.head("source_object").toString)
var extractedRow : Row = null
try {
  val filterRow = getFilterCondition(source_object,source_deep_partition,source_num_partition)
  extractedRow  =extractValuesDynamically(filterRow).get

} catch {
  case e: Exception =>
    extractedRow = null
}
if (extractedRow != null) {
  query = replaceQueryYearMonthDay(extractedRow, query, incremental)
}


// COMMAND ----------

import java.util.Calendar
val time=Calendar.getInstance().getTimeInMillis()
val tmpviewname="temporalvw_"+time+"CDP"

var dfPrcprocesado=getDataFromSource(sourcesFinal(0))
var list=query.replaceFirst("(?i)SELECT ","").split(",(?![^()]*\\))")
dfPrcprocesado=dfPrcprocesado.selectExpr(list:_*)

// COMMAND ----------

target_object = getObject(target_object).toString
val finalDFwithImposedSchema=applyDDLFromTableToDF(dfPrcprocesado, target_object)

// COMMAND ----------

val joinColsOpt = if (target_write_mode != "overwrite" && target_write_mode != "dynamic") Some(keycolumns) else None

writeWrapper(
  sourceDF = finalDFwithImposedSchema,
  conf = targetFinal,
  optimize = optimizeDelta,
  prefix = "target",
  joinCols = joinColsOpt.getOrElse(null),
  operationName = targetFinal("target_operation_name").toString
)


// COMMAND ----------

// DBTITLE 1,Logging Activity

val result = "df_origen"

if (uniqueKey.nonEmpty && step > 0)
    print(uniqueKey)
    ControlHelper().updateControlTable(spark, uniqueKey, step, s"$name: $result")

if (!error.isEmpty) {
    LogHelper().logError(s"$name: $error")
    throw new Exception(s"$name: $error")
} 

LogHelper().logInfo(s"$name: $result")  

// COMMAND ----------

val sourceObject = sourcesFinal.headOption
  .flatMap(_.get("source_object"))
  .getOrElse("")
  .toString

val jsonExitValue = s"""{
  "source_table_name": "$sourceObject",
  "target_table_name": "${targetFinal.getOrElse("target_object", "")}"
}"""

dbutils.notebook.exit(jsonExitValue)