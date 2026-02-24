// Databricks notebook source
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

//val applicationName="crp0012_APP"
//val uuid="lakeh#crp0012_APP#20251113095422936"
//val puid=null
//val referenceDate="2025/02/24" //2025/11/11
//val uniqueKey=""
//val digitalCase="crp0012"
//val optimizeDelta=false
//val params= "{\"year\": \"2025\", \"month\": \"11\", \"day\":\"18\",\"sourcetable\":\"aa001256_glb_task_sla\",\"targettable\":\"crp0012_tb_fact_x_task_sla\",\"folder\":\"origen\"}"
//val auxParams="{\"keycolumns\":[\"id_sys_id\"],\"sql_file\":\"crp0012_c_ss_source_tb_fact_x_task_sla.sql\",\"incremental\":\"true\"}"
//val sources="[{\"source_object\":\"datahub01%env%aucdatagold.it_digital.aa001256_glb_task_sla\",\"source_format\":\"Table\",\"source_deep_partition\":3,\"source_num_partitions\":0,\"source_alias\":\"aa001256_glb_task_sla\"}]"
//val target="{\"target_object\":\"datahub01%env%aucdatagold.it_digital.crp0012_tb_fact_x_task_sla\",\"target_format\":\"Table\",\"target_write_mode\":\"merge\",\"target_operation_name\":\"scd1\",\"target_dl_name_tags\":[]}"
//val name="crp0012_a_nb_e_t_l_mapping"
//val spark_properties="{\"spark.sql.legacy.ctePrecedencePolicy\":\"CORRECTED\",\"spark.sql.legacy.lpadRpadAlwaysReturnString\":\"true\"}"



// COMMAND ----------

// MAGIC %run /Shared/DAB-CRP0012/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_a_nb_arquetipo_functions

// COMMAND ----------

implicit val spark1:SparkSession=spark

// COMMAND ----------

// MAGIC %md
// MAGIC Esta celda sobrescribe los valores de los parámetros JSON con los valores proporcionados en los parámetros. Se utiliza exclusivamente por el Pipeline de Recuperación.

// COMMAND ----------

val mapper = new ObjectMapper() with ClassTagExtensions

mapper.registerModule(DefaultScalaModule)
mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

val paramsFinal: Map[String, Any] = Option(params)
  .filter(_.trim.nonEmpty)
  .map(json => mapper.readValue[Map[String, Any]](json))
  .getOrElse(Map.empty[String, Any])
var sourcesFinal: List[Map[String, Any]] = paramsFinal
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
  

// COMMAND ----------

// MAGIC %md
// MAGIC Actividad de registro

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

// MAGIC %md
// MAGIC Función para reemplazarQueryAñoMesDía

// COMMAND ----------

def replaceQueryYearMonthDay(extractedRow: Row, query: String, incremental: Boolean): String = {
  var newQuery = query

  if (incremental) {
    // Extract year, month, and day from Row
    if (extractedRow.length >= 1)
      newQuery = newQuery.replaceFirst("year +as +num_year", extractedRow.get(0).toString + " as num_year")

    if (extractedRow.length >= 2)
      newQuery = newQuery.replaceFirst("month +as +num_month", extractedRow.get(1).toString + " as num_month")

    if (extractedRow.length >= 3)
      newQuery = newQuery.replaceFirst("day +as +num_day", extractedRow.get(2).toString + " as num_day")
  }

  newQuery
}


// COMMAND ----------

// MAGIC %md
// MAGIC Función para obtener el valor del último año, mes y día para reemplazar en la consulta solo para casos de prueba.

// COMMAND ----------

def getLatestDateParts(df: DataFrame): Row = {
  val availableCols = Seq("year_load_aria", "month_load_aria", "day_load_aria").filter(df.columns.contains)

  if (availableCols.isEmpty) {
    throw new IllegalArgumentException("No date-related columns found in DataFrame.")
  }

  val orderedRow = df
    .select(availableCols.map(col): _*)
    .orderBy(availableCols.map(c => col(c).desc): _*)
    .first()

  orderedRow
}

// COMMAND ----------

// MAGIC %md
// MAGIC Función para hacer cumplir el esquema de la tabla de destino en el dataframe final

// COMMAND ----------

def convertSchemaToTarget(df: DataFrame, tableName: String)
                         (implicit spark: SparkSession): DataFrame = {

  // Get target schema from table
  val targetDF = getDataFromSource(Map("source_format" -> "Table", "source_object" -> tableName))
  val targetSchema = targetDF.schema

  // Select only matching columns
  val selectedCols = targetSchema.fieldNames.map(col)
  val dfSelected = df.select(selectedCols: _*)

  //Cast columns to target schema
  val castedCols = targetSchema.fields.map(f => col(f.name).cast(f.dataType).alias(f.name))
  dfSelected.select(castedCols: _*)
}


// COMMAND ----------

var incremental: Boolean = aux_paramsFinal("incremental").toString.toBoolean
val sql_file:String = aux_paramsFinal("sql_file").toString
val keycol_str = aux_paramsFinal.get("keycolumns")

// COMMAND ----------

var source_num_partition = sourcesFinal.head("source_num_partitions").toString.toInt
var source_deep_partition = sourcesFinal.head("source_deep_partition").toString.toInt
var source_object = sourcesFinal.head("source_object").toString
var source_format = sourcesFinal.head("source_format").toString
var target_object=targetFinal("target_object").toString
val target_operation_name=targetFinal("target_operation_name").toString
val target_write_mode = Option(targetFinal.getOrElse("target_write_mode", "")).map(_.toString).getOrElse("")
val target_enforce_schema = targetFinal.get("target_enforce_schema") match {
  case Some(value) => value.toString.toBoolean
  case None => true
}

val target_dl_name_tags = targetFinal
  .get("target_dl_name_tags")                           
  .map(_.toString)                                      
  .getOrElse("")                                         
  .split(",")                                       
  .map(_.trim)                                           
  .toList


// COMMAND ----------

// MAGIC %md
// MAGIC Aplicar propiedades de Spark

// COMMAND ----------

var spark_spark_properties = spark_properties.replaceAll("[\t]", "")
val withCommas = spark_spark_properties.replaceAll("\"\\s*\"", "\", \"")
// Convert string to DataFrame
val df = Seq(withCommas).toDF("json_col")

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
var query = readFile(s"config_$digitalCase/config_files/sql/origen/$sql_file")

// COMMAND ----------

// MAGIC %md
// MAGIC Obtener y reemplazar el último valor de año, mes y día en la consulta sql

// COMMAND ----------

// Control table parameters
val steps = 1
val currentStep = 1
var msg: String =s"$source_object"
var extractedRow: org.apache.spark.sql.Row = null
var partitionCondition:String=""
var dfPrcprocesado:DataFrame=null

if (incremental){
  if (source_format == "parquet") { // solo es parquet para los test
    dfPrcprocesado = getDataFromSource(sourcesFinal(0), filter = partitionCondition)
    extractedRow = getLatestDateParts(dfPrcprocesado)
  } else {
    val Array(year, month, day) = referenceDate.split("/").map(_.toInt)
    partitionCondition = source_deep_partition match {
      case 3 => s"year_load_aria = '$year' and month_load_aria = '$month' and day_load_aria = '$day'"
      case 2 => s"year_load_aria = '$year' and month_load_aria = '$month'"
      case 1 => s"year_load_aria = '$year'"
      case _ => throw new Exception ("Incremental table cannot have source_deep_partition=0 ")
    }

    msg = msg+s"/year=$year/month=$month/day=$day"
    println(s"Partition filter: $partitionCondition")
    println("source_format: " +source_format)

    dfPrcprocesado = getDataFromSource(sourcesFinal(0), filter = partitionCondition)
    extractedRow = Row(year, month, day)
  }
  query = replaceQueryYearMonthDay(extractedRow, query, incremental)
} else {
  dfPrcprocesado = getDataFromSource(sourcesFinal(0), filter = partitionCondition)
}

dfPrcprocesado.cache
//display(dfPrcprocesado)



// COMMAND ----------

val dataCount = dfPrcprocesado.count()

// --- Logging before exit if no data ---
if (dataCount == 0) {
  println(s"No data in source table")
  val extraInfo1 = Some(s"$source_object, RowsCopied:$dataCount")
  ControlHelper().updateControlTable(spark, uniqueKey, steps, source_object, extraInfo1)
  dbutils.notebook.exit(s"N/D No data in source table for date $referenceDate")
}

// COMMAND ----------

// MAGIC %md
// MAGIC Transformación datos entrantes (mapeo)

// COMMAND ----------

var list=query.replaceFirst("(?i)SELECT ","").split(",(?![^()]*\\))")
dfPrcprocesado=dfPrcprocesado.selectExpr(list:_*)

// COMMAND ----------

// MAGIC %md
// MAGIC Aplica el esquema de la tabla de destino al DataFrame final.

// COMMAND ----------

val finalDFwithImposedSchema = {
  if (target_enforce_schema) {
    val targetObjStr = getObject(target_object).toString
    convertSchemaToTarget(dfPrcprocesado, targetObjStr)
  } else {
    dfPrcprocesado
  }
}


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

// MAGIC %md
// MAGIC Actividad de registro

// COMMAND ----------

val result = "df_origen"

var extraInfo1: Option[Any] = Some(s"$msg, RowsCopied:$dataCount")

if (uniqueKey.nonEmpty && steps > 0)
    ControlHelper().updateControlTable(spark,uniqueKey,steps,msg,extraInfo1)

LogHelper().logInfo(s"$name: $result")

// COMMAND ----------

// MAGIC %md
// MAGIC Salida mostrando la tabla de entrada seleccionada y la tabla de destino

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